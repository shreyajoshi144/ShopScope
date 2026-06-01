import logging
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    REQUIRED_COLUMNS, NULL_THRESHOLDS,
    VALID_ORDER_STATUSES, MIN_PRICE, MAX_PRICE,
    LOG_FORMAT, LOG_LEVEL,
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.validate")

# ── Individual rule functions ─────────────────────────────────────────────────

def check_required_columns(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 1 — Required columns must exist."""
    issues = []
    required = REQUIRED_COLUMNS.get(name, [])

    for col in required:
        if col not in df.columns:
            issues.append({
                "dataset": name,
                "rule":    "required_column_missing",
                "column":  col,
                "severity":"CRITICAL",
                "detail":  f"Column '{col}' is required but not found",
            })
            logger.error(f"[CRITICAL] {name}: required column '{col}' missing")

    if not issues:
        logger.info(f"[PASS] {name}: all required columns present")

    return issues

def check_null_percentages(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 2 — Null percentage per column must be within threshold."""
    issues = []
    threshold = NULL_THRESHOLDS.get(name, 0.10)   # default 10%

    null_pct = df.isnull().mean()   # series: col → fraction null

    for col, pct in null_pct.items():
        if pct > threshold:
            severity = "CRITICAL" if pct > 0.5 else "WARNING"
            issues.append({
                "dataset":  name,
                "rule":     "high_null_percentage",
                "column":   col,
                "severity": severity,
                "detail":   f"{pct:.1%} null (threshold {threshold:.0%})",
            })
            logger.warning(f"[{severity}] {name}.{col}: {pct:.1%} null values")

    if not issues:
        logger.info(f"[PASS] {name}: null percentages within threshold")

    return issues

def check_duplicates(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 3 — Detect duplicate rows."""
    issues = []
    n_dupes = df.duplicated().sum()

    if n_dupes > 0:
        pct = n_dupes / len(df)
        issues.append({
            "dataset":  name,
            "rule":     "duplicate_rows",
            "column":   "all",
            "severity": "WARNING",
            "detail":   f"{n_dupes:,} duplicate rows ({pct:.1%} of dataset)",
        })
        logger.warning(f"[WARNING] {name}: {n_dupes:,} duplicate rows found")
    else:
        logger.info(f"[PASS] {name}: no duplicate rows")

    return issues

def check_price_ranges(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 4 — Price and payment values must be within sensible bounds."""
    issues = []

    price_cols = [c for c in df.columns if c in ("price", "freight_value", "payment_value")]

    for col in price_cols:
        below = (df[col] < MIN_PRICE).sum()
        above = (df[col] > MAX_PRICE).sum()

        if below > 0:
            issues.append({
                "dataset":  name,
                "rule":     "price_below_minimum",
                "column":   col,
                "severity": "WARNING",
                "detail":   f"{below:,} rows with {col} < {MIN_PRICE}",
            })
            logger.warning(f"[WARNING] {name}.{col}: {below:,} rows below minimum price")

        if above > 0:
            issues.append({
                "dataset":  name,
                "rule":     "price_above_maximum",
                "column":   col,
                "severity": "WARNING",
                "detail":   f"{above:,} rows with {col} > {MAX_PRICE}",
            })
            logger.warning(f"[WARNING] {name}.{col}: {above:,} rows above maximum price")

    if not issues:
        logger.info(f"[PASS] {name}: price values within range")

    return issues

def check_review_scores(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 5 — Review scores must be integers 1–5."""
    issues = []

    if "review_score" not in df.columns:
        return issues

    invalid = df["review_score"].dropna()
    invalid = invalid[~invalid.isin([1, 2, 3, 4, 5])]

    if len(invalid) > 0:
        issues.append({
            "dataset":  name,
            "rule":     "invalid_review_score",
            "column":   "review_score",
            "severity": "WARNING",
            "detail":   f"{len(invalid):,} scores not in [1,2,3,4,5]",
        })
        logger.warning(f"[WARNING] {name}: {len(invalid):,} invalid review scores")
    else:
        logger.info(f"[PASS] {name}: review scores all valid")

    return issues

def check_order_status(name: str, df: pd.DataFrame) -> list[dict]:
    """Rule 6 — order_status must be one of the known valid values."""
    issues = []

    if "order_status" not in df.columns:
        return issues

    unexpected = df[~df["order_status"].isin(VALID_ORDER_STATUSES)]["order_status"].unique()

    if len(unexpected) > 0:
        issues.append({
            "dataset":  name,
            "rule":     "invalid_order_status",
            "column":   "order_status",
            "severity": "WARNING",
            "detail":   f"Unexpected values: {list(unexpected)}",
        })
        logger.warning(f"[WARNING] {name}: unexpected order_status values: {list(unexpected)}")
    else:
        logger.info(f"[PASS] {name}: all order_status values valid")

    return issues

# ── Main validator orchestrator ───────────────────────────────────────────────

def validate_dataset(name: str, df: pd.DataFrame) -> list[dict]:
    """
    Run all validation rules against one dataset.
    Returns list of issue dicts (empty = fully valid).
    """
    logger.info(f"\nValidating: {name} ({len(df):,} rows)")

    all_issues = []
    all_issues += check_required_columns(name, df)
    all_issues += check_null_percentages(name, df)
    all_issues += check_duplicates(name, df)
    all_issues += check_price_ranges(name, df)
    all_issues += check_review_scores(name, df)
    all_issues += check_order_status(name, df)

    return all_issues


def validate_all(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:

    logger.info("=" * 60)
    logger.info("ShopScope | Module 1 | Validation Starting")
    logger.info("=" * 60)

    all_issues = []

    for name, df in datasets.items():
        issues = validate_dataset(name, df)
        all_issues.extend(issues)

    report = pd.DataFrame(all_issues) if all_issues else pd.DataFrame(
        columns=["dataset", "rule", "column", "severity", "detail"]
    )

    # ── Print summary ─────────────────────────────────────────────────────────
    logger.info("\n── Validation Report ────────────────────────────────────────")
    if report.empty:
        logger.info("ALL DATASETS PASSED VALIDATION ✓")
    else:
        criticals = (report["severity"] == "CRITICAL").sum()
        warnings  = (report["severity"] == "WARNING").sum()
        logger.info(f"Issues found: {criticals} CRITICAL, {warnings} WARNING")

        if criticals > 0:
            logger.error("CRITICAL issues must be resolved before proceeding")

        print("\n── Validation Issues ────────────────────────────────────────")
        print(report.to_string(index=False))
        print("─────────────────────────────────────────────────────────────")

    return report


def has_critical_issues(report: pd.DataFrame) -> bool:
    """
    Returns True if any CRITICAL issues were found.
    Use this to gate whether transform.py should proceed.
    """
    if report.empty:
        return False
    return (report["severity"] == "CRITICAL").any()

# ── Run directly to test validation ───────────────────────────────────────────
if __name__ == "__main__":
    from extract import load_all_datasets
    datasets = load_all_datasets()
    report = validate_all(datasets)

    if has_critical_issues(report):
        print("\nSTOP: Critical issues found. Fix data before proceeding.")
    else:
        print("\nOK: Safe to proceed to transform.py")
