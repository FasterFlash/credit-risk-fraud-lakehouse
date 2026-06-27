"""
src/bronze/ingestion.py

Production-grade Bronze ingestion using Auto Loader.
Config-driven. No hardcoded values. Structured logging throughout.
"""

import time
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import functions as F, SparkSession

from config.pipeline_config import PipelineConfig
from config.table_config import BRONZE_TABLES, TableDefinition, get_table_definition
from utils.logger import PipelineLogger


@dataclass
class IngestionResult:
    table_name:    str
    rows_written:  int
    rows_rescued:  int
    duration_secs: float
    status:        str
    error:         Optional[str] = None


class BronzeIngestion:
    """
    Ingests all Bronze tables via Auto Loader.
    One call to run_all() processes every table in BRONZE_TABLES.
    One call to ingest_table() processes a single table.
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

    def run_all(self) -> list:
        self._logger.task_start()
        results = []

        for table_name in BRONZE_TABLES:
            result = self.ingest_table(table_name)
            results.append(result)

        failed = [r for r in results if r.status == "FAILED"]
        if failed:
            self._logger.task_failed(
                RuntimeError(f"{len(failed)} table(s) failed ingestion")
            )
            raise RuntimeError(
                f"Bronze ingestion failed for: "
                f"{[r.table_name for r in failed]}"
            )

        self._logger.task_success()
        return results

    def ingest_table(self, table_name: str) -> IngestionResult:
        defn   = get_table_definition(table_name)
        target = f"{self._config.catalog}.{defn.target_table}"
        source = f"{self._config.source_landing}/{defn.source_folder}"
        chk    = f"{self._config.checkpoint_base}/bronze_{table_name}"
        schema = f"{chk}_schema"

        self._logger.info(
            f"Starting ingestion: {source} → {target}",
            table_name=table_name,
        )

        t_start = time.time()

        try:
            query = self._build_stream(
                defn   = defn,
                source = source,
                target = target,
                chk    = chk,
                schema = schema,
            )
            query.awaitTermination()

            duration     = time.time() - t_start
            rows_written = self._spark.table(target).count()
            rows_rescued = self._count_rescued(target)

            self._logger.metric(
                table_name    = table_name,
                rows_written  = rows_written,
                rows_rescued  = rows_rescued,
                duration_secs = duration,
                status        = "SUCCESS",
            )

            return IngestionResult(
                table_name    = table_name,
                rows_written  = rows_written,
                rows_rescued  = rows_rescued,
                duration_secs = duration,
                status        = "SUCCESS",
            )

        except Exception as e:
            duration = time.time() - t_start
            self._logger.error(
                f"Ingestion failed for {table_name}: {e}",
                table_name = table_name,
                error      = e,
            )
            return IngestionResult(
                table_name    = table_name,
                rows_written  = 0,
                rows_rescued  = 0,
                duration_secs = duration,
                status        = "FAILED",
                error         = str(e),
            )

    # ---------------------------------------------------------------------------
    # Private
    # ---------------------------------------------------------------------------

    def _build_stream(
        self,
        defn:   TableDefinition,
        source: str,
        target: str,
        chk:    str,
        schema: str,
    ):
        cast_map = defn.cast_map

        def process_batch(batch_df, batch_id):
            df = batch_df

            df = (
                df
                .withColumn(
                    "_source_file",
                    F.col("_metadata.file_path")
                )
                .withColumn(
                    "_ingestion_time",
                    F.current_timestamp()
                )
            )

            # Recover rescued values + cast to correct types
            if "_rescued_data" in df.columns and defn.rescued_cols:
                for col_name in defn.rescued_cols:
                    col_type = cast_map.get(col_name, "string")
                    df = df.withColumn(
                        col_name,
                        F.coalesce(
                            F.col(col_name),
                            F.get_json_object(
                                F.col("_rescued_data"),
                                f"$.{col_name}"
                            )
                        )
                    )

            # Apply all casts
            for col_name, col_type in cast_map.items():
                if col_name in df.columns:
                    df = df.withColumn(
                        col_name,
                        F.col(col_name).cast(col_type)
                    )

            writer = (
                df.write
                  .format("delta")
                  .mode("append")
                  .option("mergeSchema", str(self._config.merge_schema).lower())
            )
            if defn.partition_col:
                writer = writer.partitionBy(defn.partition_col)

            writer.saveAsTable(target)

        stream = (
            self._spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format",
                        self._config.autoloader_format)
                .option("cloudFiles.schemaLocation", schema)
                .option("cloudFiles.inferColumnTypes",
                        self._config.autoloader_infer_types)
                .option("cloudFiles.schemaEvolutionMode",
                        self._config.autoloader_schema_evolution)
                .option("cloudFiles.rescuedDataColumn",
                        self._config.autoloader_rescued_col)
                .load(source)
        )

        return (
            stream.writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", chk)
                .trigger(availableNow=self._config.trigger_available_now)
                .start()
        )

    def _count_rescued(self, target: str) -> int:
        try:
            df = self._spark.table(target)
            if "_rescued_data" in df.columns:
                return df.filter("_rescued_data IS NOT NULL").count()
            return 0
        except Exception:
            return -1