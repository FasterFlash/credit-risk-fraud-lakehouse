"""
src/utils/preflight.py

Pre-flight checks before any pipeline run.
Validates: catalog, schemas, volume, source files.
Fails fast on critical issues. Warns on non-critical.
"""

import os
from dataclasses import dataclass, field
from typing import List, Tuple

from config.pipeline_config import PipelineConfig
from utils.logger import PipelineLogger


@dataclass
class CheckResult:
    passed:   bool
    failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_failure(self, msg: str) -> None:
        self.failures.append(msg)
        self.passed = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


class PreflightChecker:
    """
    Runs all pre-flight checks before pipeline execution.
    Called by admin/00_preflight_checks notebook.
    """

    def __init__(self, spark, config: PipelineConfig, logger: PipelineLogger):
        self._spark  = spark
        self._config = config
        self._logger = logger

    def run_all(self) -> CheckResult:
        result = CheckResult(passed=True)

        self._check_catalog(result)
        self._check_schemas(result)
        self._check_volume(result)
        self._check_source_files(result)
        self._check_observability(result)
        self._check_checkpoints_dir(result)

        if result.failures:
            for f in result.failures:
                self._logger.error(f"PRE-FLIGHT FAILED: {f}")
            raise RuntimeError(
                f"Pre-flight checks failed with {len(result.failures)} error(s): "
                f"{result.failures}"
            )

        for w in result.warnings:
            self._logger.warn(f"PRE-FLIGHT WARN: {w}")

        self._logger.info(
            f"Pre-flight passed — "
            f"{len(result.warnings)} warning(s)"
        )
        return result

    # ---------------------------------------------------------------------------
    # Individual checks
    # ---------------------------------------------------------------------------

    def _check_catalog(self, result: CheckResult) -> None:
        try:
            catalogs = [
                r.catalog
                for r in self._spark.sql("SHOW CATALOGS").collect()
            ]
            if self._config.catalog not in catalogs:
                result.add_failure(
                    f"Catalog '{self._config.catalog}' not found. "
                    f"Available: {catalogs}"
                )
        except Exception as e:
            result.add_failure(f"Catalog check failed: {e}")

    def _check_schemas(self, result: CheckResult) -> None:
        try:
            existing = [
                r.databaseName
                for r in self._spark.sql(
                    f"SHOW SCHEMAS IN {self._config.catalog}"
                ).collect()
            ]
            for schema in self._config.required_schemas:
                if schema not in existing:
                    result.add_failure(
                        f"Schema '{self._config.catalog}.{schema}' not found."
                    )
        except Exception as e:
            result.add_failure(f"Schema check failed: {e}")

    def _check_volume(self, result: CheckResult) -> None:
        try:
            if not os.path.exists(self._config.volume_path):
                result.add_failure(
                    f"Volume path not accessible: {self._config.volume_path}"
                )
        except Exception as e:
            result.add_failure(f"Volume check failed: {e}")

    def _check_source_files(self, result: CheckResult) -> None:
        for folder in self._config.required_source_folders:
            path = f"{self._config.source_landing}/{folder}"
            try:
                if not os.path.exists(path):
                    result.add_failure(
                        f"Source folder missing: {path}"
                    )
                else:
                    file_count = sum(
                        len(files)
                        for _, _, files in os.walk(path)
                    )
                    if file_count == 0:
                        result.add_warning(
                            f"Source folder empty: {path}"
                        )
            except Exception as e:
                result.add_failure(
                    f"Source file check failed for {folder}: {e}"
                )

    def _check_observability(self, result: CheckResult) -> None:
        try:
            self._spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self._config.observability}"
            )
        except Exception as e:
            result.add_warning(
                f"Could not ensure observability schema: {e}"
            )

    def _check_checkpoints_dir(self, result: CheckResult) -> None:
        chk = self._config.checkpoint_base
        try:
            if not os.path.exists(chk):
                os.makedirs(chk, exist_ok=True)
        except Exception as e:
            result.add_warning(
                f"Could not verify checkpoint directory {chk}: {e}"
            )