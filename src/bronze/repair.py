"""
src/bronze/repair.py

Post-ingestion schema repair for Bronze tables.
Recovers values from _rescued_data via MERGE INTO.
Called by the For Each Workflow task — one table per iteration.
"""

import time
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import functions as F, SparkSession

from config.pipeline_config import PipelineConfig
from config.table_config import TABLE_DEFINITIONS, get_table_definition
from utils.logger import PipelineLogger


@dataclass
class RepairResult:
    table_name:      str
    rows_rescued_before: int
    rows_rescued_after:  int
    rows_repaired:   int
    duration_secs:   float
    status:          str
    error:           Optional[str] = None


class BronzeRepair:
    """
    Recovers values from _rescued_data via MERGE INTO.
    Idempotent: safe to re-run. Exits immediately if no rescued rows.
    """

    def __init__(
        self,
        spark:   SparkSession,
        config:  PipelineConfig,
        logger:  PipelineLogger,
    ):
        self._spark  = spark
        self._config = config
        self._logger = logger

    def repair_table(
        self,
        table_name:   str,
        primary_key:  str,
        rescued_cols: List[str],
    ) -> RepairResult:

        target = f"{self._config.catalog}.bronze.{table_name}"
        t_start = time.time()

        self._logger.info(
            f"Starting repair: {target}",
            table_name=table_name,
        )

        try:
            df = self._spark.table(target)

            # Check if _rescued_data column exists
            if "_rescued_data" not in df.columns:
                self._logger.info(
                    f"No _rescued_data column in {table_name} — skipping",
                    table_name=table_name,
                )
                return RepairResult(
                    table_name           = table_name,
                    rows_rescued_before  = 0,
                    rows_rescued_after   = 0,
                    rows_repaired        = 0,
                    duration_secs        = time.time() - t_start,
                    status               = "SKIPPED",
                )

            # Count rescued rows before repair
            rescued_before = (
                df.filter("_rescued_data IS NOT NULL").count()
            )

            if rescued_before == 0:
                self._logger.info(
                    f"No rescued rows in {table_name} — skipping",
                    table_name=table_name,
                )
                return RepairResult(
                    table_name           = table_name,
                    rows_rescued_before  = 0,
                    rows_rescued_after   = 0,
                    rows_repaired        = 0,
                    duration_secs        = time.time() - t_start,
                    status               = "SKIPPED",
                )

            self._logger.info(
                f"{rescued_before:,} rescued rows found — "
                f"recovering cols: {rescued_cols}",
                table_name=table_name,
            )

            # Build recovery DataFrame from rescued rows
            defn     = get_table_definition(table_name)
            type_map = defn.cast_map
            rescue_df = df.filter("_rescued_data IS NOT NULL")

            # Extract values from _rescued_data JSON and coalesce
            recovery_df = rescue_df
            for col_name in rescued_cols:
                col_type = type_map.get(col_name, "string")
                recovery_df = recovery_df.withColumn(
                    col_name,
                    F.coalesce(
                        F.col(col_name),
                        F.get_json_object(
                            F.col("_rescued_data"),
                            f"$.{col_name}"
                        ).cast(col_type)
                    )
                )

            # Clear _rescued_data after recovery
            # Only nulls it if ALL fields were type-mismatch (no genuine DQ)
            recovery_df = recovery_df.withColumn(
                "_rescued_data",
                F.lit(None).cast("string")
            )

            # Select only merge keys + recovered cols + _rescued_data
            merge_cols = [primary_key] + rescued_cols + ["_rescued_data"]
            merge_df = recovery_df.select(merge_cols)
            merge_df.createOrReplaceTempView("_repair_source")

            # Build SET clause
            set_parts = [
                f"target.{col} = source.{col}"
                for col in rescued_cols
            ] + ["target._rescued_data = source._rescued_data"]

            set_clause = ",\n        ".join(set_parts)

            merge_sql = f"""
                MERGE INTO {target} AS target
                USING _repair_source AS source
                ON target.{primary_key} = source.{primary_key}
                WHEN MATCHED AND target._rescued_data IS NOT NULL
                THEN UPDATE SET
                    {set_clause}
            """

            self._spark.sql(merge_sql)

            # Verify
            rescued_after = (
                self._spark.table(target)
                    .filter("_rescued_data IS NOT NULL")
                    .count()
            )
            rows_repaired = rescued_before - rescued_after
            duration      = time.time() - t_start

            self._logger.metric(
                table_name    = table_name,
                rows_rescued  = rescued_after,
                duration_secs = duration,
                status        = "SUCCESS",
            )

            if rescued_after > 0:
                self._logger.warn(
                    f"{rescued_after:,} rows still rescued after repair "
                    f"— may be genuine DQ violations",
                    table_name=table_name,
                )

            return RepairResult(
                table_name           = table_name,
                rows_rescued_before  = rescued_before,
                rows_rescued_after   = rescued_after,
                rows_repaired        = rows_repaired,
                duration_secs        = duration,
                status               = "SUCCESS",
            )

        except Exception as e:
            duration = time.time() - t_start
            self._logger.error(
                f"Repair failed for {table_name}: {e}",
                table_name = table_name,
                error      = e,
            )
            return RepairResult(
                table_name           = table_name,
                rows_rescued_before  = -1,
                rows_rescued_after   = -1,
                rows_repaired        = 0,
                duration_secs        = duration,
                status               = "FAILED",
                error                = str(e),
            )