"""
src/config/pipeline_config.py

Central configuration for the Credit Risk & Fraud Detection Lakehouse.
All environment-specific values live here.
No values are hardcoded in notebooks or pipeline modules.
"""

from dataclasses import dataclass, field
from typing import Optional
import os


@dataclass(frozen=True)
class PipelineConfig:
    """
    Immutable pipeline configuration.
    Frozen to prevent accidental mutation during a run.
    Override via environment variables for dev/stg/prod promotion.
    """

    # ---------------------------------------------------------------------------
    # Unity Catalog
    # ---------------------------------------------------------------------------
    catalog: str = os.getenv("PIPELINE_CATALOG", "credit_risk_lakehouse")

    # Schemas
    schema_bronze:       str = "bronze"
    schema_silver:       str = "silver"
    schema_gold:         str = "gold"
    schema_ml_features:  str = "ml_features"
    schema_ml_models:    str = "ml_models"
    schema_observability:str = "observability"
    schema_raw_ingestion:str = "raw_ingestion"

    # ---------------------------------------------------------------------------
    # Volumes
    # ---------------------------------------------------------------------------
    volume_raw:          str = "landing_raw"

    # ---------------------------------------------------------------------------
    # Derived paths (computed from catalog + schema + volume)
    # ---------------------------------------------------------------------------
    @property
    def bronze(self) -> str:
        return f"{self.catalog}.{self.schema_bronze}"

    @property
    def silver(self) -> str:
        return f"{self.catalog}.{self.schema_silver}"

    @property
    def gold(self) -> str:
        return f"{self.catalog}.{self.schema_gold}"

    @property
    def observability(self) -> str:
        return f"{self.catalog}.{self.schema_observability}"

    @property
    def volume_path(self) -> str:
        return (
            f"/Volumes/{self.catalog}"
            f"/{self.schema_raw_ingestion}"
            f"/{self.volume_raw}"
        )

    @property
    def checkpoint_base(self) -> str:
        return f"{self.volume_path}/_checkpoints"

    @property
    def source_landing(self) -> str:
        return self.volume_path

    # ---------------------------------------------------------------------------
    # Observability
    # ---------------------------------------------------------------------------
    observability_table: str = "pipeline_runs"

    @property
    def observability_table_fqn(self) -> str:
        return f"{self.observability}.{self.observability_table}"

    # ---------------------------------------------------------------------------
    # Auto Loader defaults
    # ---------------------------------------------------------------------------
    autoloader_format:          str = "parquet"
    autoloader_infer_types:     str = "true"
    autoloader_schema_evolution:str = "addNewColumns"
    autoloader_rescued_col:     str = "_rescued_data"

    # ---------------------------------------------------------------------------
    # Pipeline behaviour
    # ---------------------------------------------------------------------------
    trigger_available_now: bool = True
    merge_schema:          bool = True

    # ---------------------------------------------------------------------------
    # Expected schemas (for pre-flight validation)
    # ---------------------------------------------------------------------------
    required_schemas: tuple = (
        "bronze", "silver", "gold",
        "ml_features", "ml_models",
        "observability", "raw_ingestion",
    )

    required_source_folders: tuple = (
        "transactions",
        "customers",
        "loan_applications",
        "bureau_pulls",
        "underwriting_decisions",
        "disbursements",
        "repayment_schedules",
        "repayment_events",
        "nach_bounces",
        "cdc/loan_status",
        "cdc/customer_updates",
        "loan_personal_detail",
        "loan_home_detail",
        "loan_auto_detail",
        "credit_card_detail",
    )