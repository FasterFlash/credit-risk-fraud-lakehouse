"""
src/config/table_config.py

Single source of truth for all Bronze table definitions.
Defines per-table: source folder, primary key, cast map, rescued columns.

Cast map: columns that need explicit type casting after Auto Loader ingestion.
Rescued cols: columns known to land in _rescued_data due to type conflicts.

No table metadata lives in notebooks.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class TableDefinition:
    source_folder:  str
    target_table:   str
    primary_key:    str
    cast_map:       Dict[str, str]
    rescued_cols:   List[str] = field(default_factory=list)
    partition_col:  Optional[str] = None
    description:    str = ""


# ---------------------------------------------------------------------------
# Bronze table definitions
# ---------------------------------------------------------------------------

BRONZE_TABLES: Dict[str, TableDefinition] = {

    "transactions": TableDefinition(
        source_folder  = "transactions",
        target_table   = "bronze.transactions",
        primary_key    = "transaction_id",
        partition_col  = "transaction_date",
        description    = "20M+ daily transactions with pre-computed fraud signals",
        cast_map = {
            "amount":                    "double",
            "distance_from_home_km":     "double",
            "account_balance_before":    "double",
            "account_balance_after":     "double",
            "amount_vs_30d_avg_ratio":   "double",
            "time_since_last_txn_mins":  "double",
            "velocity_1hr":              "long",
            "velocity_24hr":             "long",
            "is_reversal":               "boolean",
            "is_international":          "boolean",
            "is_new_device":             "boolean",
            "is_new_merchant_category":  "boolean",
            "is_night_transaction":      "boolean",
            "is_fraud":                  "boolean",
        },
        rescued_cols = [],
    ),

    "customers": TableDefinition(
        source_folder  = "customers",
        target_table   = "bronze.customers",
        primary_key    = "customer_id",
        description    = "Customer master, KYC profiles, and credit profiles",
        cast_map = {
            "monthly_income":             "double",
            "annual_income_declared":     "double",
            "cibil_score":                "double",
            "total_outstanding_amount":   "double",
            "total_overdue_amount":       "double",
            "existing_emi_total":         "double",
            "foir_current":               "double",
            "months_since_last_default":  "double",
            "total_active_loans":         "long",
            "total_active_credit_cards":  "long",
            "hard_pull_count_last_30d":   "long",
            "hard_pull_count_last_60d":   "long",
            "hard_pull_count_last_90d":   "long",
            "max_dpd_last_12m":           "long",
            "max_dpd_ever":               "long",
            "written_off_accounts":       "long",
            "settled_accounts":           "long",
            "is_active":                  "boolean",
            "pan_verified":               "boolean",
            "aadhaar_verified":           "boolean",
            "address_proof_verified":     "boolean",
            "income_proof_verified":      "boolean",
            "video_kyc_completed":        "boolean",
            "is_politically_exposed_person": "boolean",
            "is_high_risk_customer":      "boolean",
            "is_new_to_credit":           "boolean",
        },
        rescued_cols = [],
    ),

    "loan_applications": TableDefinition(
        source_folder  = "loan_applications",
        target_table   = "bronze.loan_applications",
        primary_key    = "application_id",
        description    = "Loan applications across all product types",
        cast_map = {
            "applied_amount":            "double",
            "applied_tenure_months":     "double",
            "declared_monthly_income":   "double",
            "existing_emi_obligations":  "double",
            "foir_at_application":       "double",
            "is_new_vehicle":            "boolean",
            "processing_fee_waived":     "boolean",
        },
        rescued_cols = ["applied_amount", "is_new_vehicle"],
    ),

    "bureau_pulls": TableDefinition(
        source_folder  = "bureau_pulls",
        target_table   = "bronze.bureau_pulls",
        primary_key    = "bureau_pull_id",
        description    = "CIBIL hard pull records per loan application",
        cast_map = {
            "cibil_score":                "double",
            "months_since_last_default":  "double",
            "total_outstanding_amount":   "double",
            "total_overdue_amount":       "double",
            "total_active_loans":         "long",
            "total_active_credit_cards":  "long",
            "hard_pull_count_last_30d":   "long",
            "hard_pull_count_last_60d":   "long",
            "hard_pull_count_last_90d":   "long",
            "max_dpd_last_12m":           "long",
            "max_dpd_ever":               "long",
            "written_off_accounts":       "long",
            "settled_accounts":           "long",
            "is_new_to_credit":           "boolean",
        },
        rescued_cols = ["months_since_last_default"],
    ),

    "underwriting_decisions": TableDefinition(
        source_folder  = "underwriting_decisions",
        target_table   = "bronze.underwriting_decisions",
        primary_key    = "decision_id",
        description    = "Underwriting decisions with approved terms",
        cast_map = {
            "approved_amount":           "double",
            "approved_tenure_months":    "double",
            "interest_rate_annual":      "double",
            "emi_amount":                "double",
            "processing_fee":            "double",
            "processing_fee_gst":        "double",
            "loan_insurance_amount":     "double",
            "cibil_score_at_decision":   "double",
            "foir_after_approval":       "double",
            "ltv_ratio":                 "double",
        },
        rescued_cols = [
            "approved_amount", "approved_tenure_months",
            "interest_rate_annual", "emi_amount",
            "processing_fee", "processing_fee_gst",
            "foir_after_approval",
        ],
    ),

    "disbursements": TableDefinition(
        source_folder  = "disbursements",
        target_table   = "bronze.disbursements",
        primary_key    = "disbursement_id",
        description    = "Loan disbursement records with NACH registration",
        cast_map = {
            "disbursed_amount":  "double",
            "first_emi_amount":  "double",
        },
        rescued_cols = [],
    ),

    "repayment_schedules": TableDefinition(
        source_folder  = "repayment_schedules",
        target_table   = "bronze.repayment_schedules",
        primary_key    = "schedule_id",
        description    = "Amortization schedule — all future EMIs at disbursement",
        cast_map = {
            "installment_number":         "long",
            "opening_principal":          "double",
            "emi_amount":                 "double",
            "principal_component":        "double",
            "interest_component":         "double",
            "closing_principal":          "double",
            "cumulative_principal_paid":  "double",
            "cumulative_interest_paid":   "double",
            "paid_amount":                "double",
            "is_paid":                    "boolean",
        },
        rescued_cols = [],
    ),

    "repayment_events": TableDefinition(
        source_folder  = "repayment_events",
        target_table   = "bronze.repayment_events",
        primary_key    = "repayment_event_id",
        description    = "Monthly EMI actual payment outcomes",
        cast_map = {
            "amount_due":                   "double",
            "amount_paid":                  "double",
            "shortfall":                    "double",
            "penalty_charged":              "double",
            "penal_interest_rate":          "double",
            "days_late":                    "long",
            "dpd_at_event":                 "long",
            "installment_number":           "long",
            "consecutive_missed_at_event":  "long",
            "is_partial":                   "boolean",
            "is_missed":                    "boolean",
            "is_on_time":                   "boolean",
            "within_grace_period":          "boolean",
            "nach_presented":               "boolean",
            "grace_period_used":            "boolean",
            "salary_credited_this_month":   "boolean",
        },
        rescued_cols = [],
    ),

    "nach_bounces": TableDefinition(
        source_folder  = "nach_bounces",
        target_table   = "bronze.nach_bounces",
        primary_key    = "bounce_id",
        description    = "NACH auto-debit bounce events with return codes",
        cast_map = {
            "amount_attempted":          "double",
            "bank_charges_applied":      "double",
            "gst_on_charges":            "double",
            "account_balance_at_debit":  "double",
            "consecutive_bounce_count":  "long",
            "customer_notified_sms":     "boolean",
            "customer_notified_email":   "boolean",
            "retry_attempted":           "boolean",
            "is_first_bounce":           "boolean",
        },
        rescued_cols = [],
    ),

    "cdc_loan_status": TableDefinition(
        source_folder  = "cdc/loan_status",
        target_table   = "bronze.cdc_loan_status",
        primary_key    = "cdc_event_id",
        description    = "CDC stream for loan status changes (SCD Type 2 source)",
        cast_map = {
            "__sequence_number":          "long",
            "before_current_dpd":         "long",
            "before_outstanding_total":   "double",
            "before_consecutive_missed":  "long",
            "after_current_dpd":          "long",
            "after_outstanding_total":    "double",
            "after_consecutive_missed":   "long",
            "is_npa_trigger":             "boolean",
            "is_month_end_batch":         "boolean",
        },
        rescued_cols = ["after_outstanding_total"],
    ),

    "cdc_customer_updates": TableDefinition(
        source_folder  = "cdc/customer_updates",
        target_table   = "bronze.cdc_customer_updates",
        primary_key    = "cdc_event_id",
        description    = "CDC stream for customer master changes",
        cast_map = {
            "__sequence_number":      "long",
            "before_monthly_income":  "double",
            "after_monthly_income":   "double",
        },
        rescued_cols = ["before_monthly_income", "after_monthly_income"],
    ),

    "loan_personal_detail": TableDefinition(
        source_folder  = "loan_personal_detail",
        target_table   = "bronze.loan_personal_detail",
        primary_key    = "loan_account_id",
        description    = "Personal loan type-specific attributes",
        cast_map = {
            "insurance_premium":         "double",
            "prepayment_penalty_rate":   "double",
            "foreclosure_charges_pct":   "double",
            "lock_in_period_months":     "long",
            "insurance_opted":           "boolean",
        },
        rescued_cols = ["insurance_premium"],
    ),

    "loan_home_detail": TableDefinition(
        source_folder  = "loan_home_detail",
        target_table   = "bronze.loan_home_detail",
        primary_key    = "loan_account_id",
        description    = "Home loan details with LTV and RERA compliance",
        cast_map = {
            "property_value":          "double",
            "ltv_ratio":               "double",
            "co_applicant_income":     "double",
            "spread_over_base":        "double",
            "current_effective_rate":  "double",
            "insurance_premium":       "double",
            "is_rera_approved":        "boolean",
            "insurance_opted":         "boolean",
        },
        rescued_cols = ["co_applicant_income"],
    ),

    "loan_auto_detail": TableDefinition(
        source_folder  = "loan_auto_detail",
        target_table   = "bronze.loan_auto_detail",
        primary_key    = "loan_account_id",
        description    = "Auto loan details with vehicle and depreciation info",
        cast_map = {
            "manufacturing_year":        "long",
            "ex_showroom_price":         "double",
            "on_road_price":             "double",
            "down_payment_amount":       "double",
            "ltv_ratio":                 "double",
            "current_market_value":      "double",
            "depreciation_rate_annual":  "double",
            "is_new_vehicle":            "boolean",
            "hypothecation_noted_in_rc": "boolean",
        },
        rescued_cols = [],
    ),

    "credit_card_detail": TableDefinition(
        source_folder  = "credit_card_detail",
        target_table   = "bronze.credit_card_detail",
        primary_key    = "card_account_id",
        description    = "Credit card account details with billing cycle state",
        cast_map = {
            "credit_limit":              "double",
            "available_credit":          "double",
            "current_outstanding":       "double",
            "current_cycle_balance":     "double",
            "next_cycle_balance":        "double",
            "last_statement_balance":    "double",
            "min_amount_due":            "double",
            "total_amount_due":          "double",
            "last_payment_amount":       "double",
            "revolving_balance":         "double",
            "revolving_apr":             "double",
            "utilization_ratio":         "double",
            "statement_day":             "long",
            "payment_due_day":           "long",
            "reward_points":             "long",
            "overlimit_count_lifetime":  "long",
            "is_overlimit":              "boolean",
            "international_enabled":     "boolean",
            "contactless_enabled":       "boolean",
        },
        rescued_cols = [],
    ),
}


def get_table_definition(table_name: str) -> TableDefinition:
    """Returns TableDefinition for a given table name. Raises if not found."""
    if table_name not in BRONZE_TABLES:
        raise KeyError(
            f"Table '{table_name}' not found in BRONZE_TABLES. "
            f"Available: {list(BRONZE_TABLES.keys())}"
        )
    return BRONZE_TABLES[table_name]


def get_rescued_tables() -> Dict[str, TableDefinition]:
    """Returns only tables that have known rescued columns."""
    return {
        name: defn
        for name, defn in BRONZE_TABLES.items()
        if defn.rescued_cols
    }