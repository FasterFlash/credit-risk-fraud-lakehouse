"""
src/utils/logger.py

Structured pipeline logger.
Writes every log record to credit_risk_lakehouse.observability.pipeline_runs.
No print statements in production code — all output goes through this logger.
"""

import uuid
import traceback
from datetime import datetime, timezone
from typing import Optional

from config.pipeline_config import PipelineConfig
from config.observability_config import ObservabilityConfig


class PipelineLogger:
    """
    Structured logger that writes to the observability Delta table.

    Usage:
        logger = PipelineLogger(spark, config, task_name="bronze_ingestion")
        logger.info("Starting ingestion", table_name="transactions")
        logger.metric("transactions", rows_read=1000, rows_written=995)
        logger.error("Failed", table_name="transactions", error=e)
    """

    def __init__(
        self,
        spark,
        config: PipelineConfig,
        task_name: str,
        job_run_id: Optional[str] = None,
        notebook_path: Optional[str] = None,
    ):
        self._spark         = spark
        self._config        = config
        self._obs_config    = ObservabilityConfig()
        self._task_name     = task_name
        self._run_id        = str(uuid.uuid4())
        self._job_run_id    = job_run_id or self._get_job_run_id()
        self._notebook_path = notebook_path or self._get_notebook_path()

        self._ensure_observability_table()

    # ---------------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------------

    def info(
        self,
        message: str,
        table_name: Optional[str] = None,
    ) -> None:
        self._write(
            level     = self._obs_config.LEVEL_INFO,
            message   = message,
            table_name= table_name,
            status    = "RUNNING",
        )

    def warn(
        self,
        message: str,
        table_name: Optional[str] = None,
    ) -> None:
        self._write(
            level     = self._obs_config.LEVEL_WARN,
            message   = message,
            table_name= table_name,
            status    = "WARN",
        )

    def error(
        self,
        message: str,
        table_name: Optional[str] = None,
        error: Optional[Exception] = None,
    ) -> None:
        error_message = (
            traceback.format_exc() if error else None
        )
        self._write(
            level         = self._obs_config.LEVEL_ERROR,
            message       = message,
            table_name    = table_name,
            status        = "FAILED",
            error_message = error_message,
        )

    def metric(
        self,
        table_name: str,
        rows_read:    int = 0,
        rows_written: int = 0,
        rows_rescued: int = 0,
        duration_secs:float = 0.0,
        status:       str = "SUCCESS",
    ) -> None:
        rescued_pct = (
            rows_rescued / rows_read
            if rows_read > 0 else 0.0
        )
        message = (
            f"table={table_name} "
            f"read={rows_read:,} "
            f"written={rows_written:,} "
            f"rescued={rows_rescued:,} "
            f"rescued_pct={rescued_pct:.2%} "
            f"duration={duration_secs:.1f}s"
        )
        self._write(
            level         = self._obs_config.LEVEL_METRIC,
            message       = message,
            table_name    = table_name,
            rows_read     = rows_read,
            rows_written  = rows_written,
            rows_rescued  = rows_rescued,
            rescued_pct   = rescued_pct,
            duration_secs = duration_secs,
            status        = status,
        )

        # Warn if rescued exceeds threshold
        if rescued_pct > self._obs_config.max_rescued_pct:
            self.warn(
                f"Rescued pct {rescued_pct:.2%} exceeds threshold "
                f"{self._obs_config.max_rescued_pct:.2%}",
                table_name=table_name,
            )

    def task_start(self) -> None:
        self._write(
            level   = self._obs_config.LEVEL_INFO,
            message = f"Task started: {self._task_name}",
            status  = "RUNNING",
        )

    def task_success(self) -> None:
        self._write(
            level   = self._obs_config.LEVEL_INFO,
            message = f"Task completed: {self._task_name}",
            status  = "SUCCESS",
        )

    def task_failed(self, error: Exception) -> None:
        self._write(
            level         = self._obs_config.LEVEL_ERROR,
            message       = f"Task failed: {self._task_name}",
            status        = "FAILED",
            error_message = traceback.format_exc(),
        )

    # ---------------------------------------------------------------------------
    # Private helpers
    # ---------------------------------------------------------------------------

    def _write(
        self,
        level:          str,
        message:        str,
        table_name:     Optional[str]   = None,
        rows_read:      Optional[int]   = None,
        rows_written:   Optional[int]   = None,
        rows_rescued:   Optional[int]   = None,
        rescued_pct:    Optional[float] = None,
        duration_secs:  Optional[float] = None,
        status:         Optional[str]   = None,
        error_message:  Optional[str]   = None,
    ) -> None:
        record = {
            "run_id":        self._run_id,
            "job_run_id":    self._job_run_id,
            "task_name":     self._task_name,
            "table_name":    table_name,
            "log_level":     level,
            "message":       message,
            "rows_read":     rows_read,
            "rows_written":  rows_written,
            "rows_rescued":  rows_rescued,
            "rescued_pct":   rescued_pct,
            "duration_secs": duration_secs,
            "status":        status,
            "error_message": error_message,
            "logged_at":     datetime.now(timezone.utc).isoformat(),
            "notebook_path": self._notebook_path,
        }

        try:
            df = self._spark.createDataFrame([record])
            (
                df.write
                  .format("delta")
                  .mode("append")
                  .saveAsTable(self._config.observability_table_fqn)
            )
        except Exception:
            # Never let logging failures crash the pipeline
            pass

    def _ensure_observability_table(self) -> None:
        obs_config = ObservabilityConfig()
        self._spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self._config.observability}"
        )
        self._spark.sql(
            obs_config.table_ddl.format(
                table_fqn=self._config.observability_table_fqn
            )
        )

    def _get_job_run_id(self) -> str:
        try:
            ctx = (
                self._spark.sparkContext
                    .getConf()
                    .get("spark.databricks.job.runId", "interactive")
            )
            return ctx
        except Exception:
            return "interactive"

    def _get_notebook_path(self) -> str:
        try:
            from dbruntime.dbutils import DBUtils
            dbutils = DBUtils(self._spark)
            return (
                dbutils.notebook.entry_point
                    .getDbutils()
                    .notebook()
                    .getContext()
                    .notebookPath()
                    .get()
            )
        except Exception:
            return "unknown"