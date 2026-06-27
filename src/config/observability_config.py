"""
src/config/observability_config.py

Observability configuration — log levels, table schema, alert thresholds.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ObservabilityConfig:

    # Log levels
    LEVEL_INFO:   str = "INFO"
    LEVEL_WARN:   str = "WARN"
    LEVEL_ERROR:  str = "ERROR"
    LEVEL_METRIC: str = "METRIC"

    # Alert thresholds
    max_rescued_pct:      float = 0.05   # alert if >5% rows rescued
    max_duration_seconds: int   = 3600   # alert if task runs >1 hour
    min_rows_expected:    int   = 1      # alert if 0 rows written

    # Observability table DDL
    table_ddl: str = """
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            run_id          STRING        NOT NULL,
            job_run_id      STRING,
            task_name       STRING        NOT NULL,
            table_name      STRING,
            log_level       STRING        NOT NULL,
            message         STRING,
            rows_read       LONG,
            rows_written    LONG,
            rows_rescued    LONG,
            rescued_pct     DOUBLE,
            duration_secs   DOUBLE,
            status          STRING,
            error_message   STRING,
            logged_at       TIMESTAMP     NOT NULL,
            notebook_path   STRING
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'quality' = 'observability'
        )
    """