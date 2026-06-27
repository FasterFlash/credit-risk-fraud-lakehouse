# Databricks notebook source
# admin/00_preflight_checks
#
# Task 0 in the Workflow DAG.
# Runs before Bronze ingestion.
# Fails fast if catalog, schemas, volume, or source files are missing.
# All output goes to observability.pipeline_runs — no print statements.

import sys
sys.path.insert(0, "/Workspace/Repos/tarun220505@hotmail.com/credit-risk-fraud-lakehouse/src")

from config.pipeline_config import PipelineConfig
from utils.logger import PipelineLogger
from utils.preflight import PreflightChecker

config  = PipelineConfig()
logger  = PipelineLogger(spark, config, task_name="preflight_checks")
checker = PreflightChecker(spark, config, logger)

logger.task_start()
result = checker.run_all()
logger.task_success()

# Surface result to Workflow UI
dbutils.notebook.exit("PREFLIGHT_PASSED")