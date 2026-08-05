"""Bronze-layer data quality checks (report-only; does not mutate tables)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

AUDIT_COLS = frozenset({"ingestion_timestamp", "source_file_name"})


def business_columns(df: DataFrame) -> list[str]:
    """Return non-audit column names for a bronze DataFrame."""
    return [c for c in df.schema.fieldNames() if c not in AUDIT_COLS]


def find_duplicates(df: DataFrame) -> DataFrame:
    """Groups of identical business-column rows that appear more than once."""
    biz_cols = business_columns(df)
    return (
        df.select(biz_cols)
        .groupBy(biz_cols)
        .agg(F.count("*").alias("duplicate_count"))
        .filter(F.col("duplicate_count") > 1)
        .orderBy(F.col("duplicate_count").desc())
    )


def duplicate_summary(df: DataFrame) -> dict[str, int]:
    """Return dupe_groups, dupe_rows, rows_to_drop for a bronze DataFrame."""
    dupes_df = find_duplicates(df)
    agg = dupes_df.agg(
        F.count("*").alias("dupe_groups"),
        F.coalesce(F.sum("duplicate_count"), F.lit(0)).alias("dupe_rows"),
    ).collect()[0]
    dupe_groups = int(agg["dupe_groups"])
    dupe_rows = int(agg["dupe_rows"])
    return {
        "dupe_groups": dupe_groups,
        "dupe_rows": dupe_rows,
        "rows_to_drop": dupe_rows - dupe_groups,
        "total_rows": df.count(),
    }


def null_severity(null_pct: float, high_threshold_pct: float = 10.0) -> str:
    """Classify null percentage severity. HIGH when pct > threshold, else MEDIUM."""
    return "HIGH" if null_pct > high_threshold_pct else "MEDIUM"


def null_analysis(
    df: DataFrame,
    *,
    high_threshold_pct: float = 10.0,
) -> list[dict[str, Any]]:
    """Per-column null counts/percentages for business columns with severity."""
    biz_cols = business_columns(df)
    total = df.count()
    if total == 0 or not biz_cols:
        return []

    null_row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in biz_cols]).collect()[0].asDict()
    results: list[dict[str, Any]] = []
    for col_name, n in null_row.items():
        if n and n > 0:
            pct = round(n / total * 100, 1)
            results.append(
                {
                    "column": col_name,
                    "null_count": int(n),
                    "null_pct": pct,
                    "severity": null_severity(pct, high_threshold_pct),
                    "total_rows": total,
                }
            )
    return results


def parse_date(col_name: str):
    """Parse string dates in MM/dd/yyyy or yyyy-MM-dd (matches DQ notebook)."""
    return F.coalesce(
        F.expr(f"try_to_date(`{col_name}`, 'MM/dd/yyyy')"),
        F.expr(f"try_to_date(`{col_name}`, 'yyyy-MM-dd')"),
    )


def try_num(col_name: str):
    """Safe cast to DOUBLE."""
    return F.expr(f"try_cast(`{col_name}` AS DOUBLE)")


def count_claims_loss_after_reported(claims: DataFrame) -> int:
    return claims.filter(parse_date("date_of_loss") > parse_date("reported_date")).count()


def count_claims_negative_incurred(claims: DataFrame) -> int:
    return claims.filter(try_num("incurred_amount") < 0).count()


def count_claims_paid_exceeds_incurred(claims: DataFrame) -> int:
    return claims.filter(try_num("paid_to_date") > try_num("incurred_amount")).count()


def count_premiums_bad_dates(premiums: DataFrame) -> int:
    return premiums.filter(parse_date("policy_start_date") >= parse_date("policy_end_date")).count()


def count_premiums_non_positive_sum_insured(premiums: DataFrame) -> int:
    return premiums.filter(try_num("sum_insured") <= 0).count()


def count_premiums_non_positive_annual_premium(premiums: DataFrame) -> int:
    return premiums.filter(try_num("annual_premium") <= 0).count()


def count_events_start_after_end(events: DataFrame) -> int:
    # Matches notebook: parsed start compared to raw end_date column
    return events.filter(parse_date("start_date") > F.col("end_date")).count()


def count_orphan_claim_policies(claims: DataFrame, premiums: DataFrame) -> int:
    return claims.join(premiums.select("policy_id").distinct(), "policy_id", "left_anti").count()


def count_orphan_claim_events(claims: DataFrame, events: DataFrame) -> int:
    return (
        claims.filter(F.col("event_id").isNotNull())
        .join(events.select("event_id").distinct(), "event_id", "left_anti")
        .count()
    )


def count_orphan_premium_postcodes(premiums: DataFrame, risk_zone: DataFrame) -> int:
    return (
        premiums.withColumn("postcode", F.col("postcode").cast("int"))
        .join(risk_zone.select("postcode").distinct(), "postcode", "left_anti")
        .count()
    )


def risk_zone_uniqueness(risk_zone: DataFrame) -> dict[str, Any]:
    """Check that risk zone lookup has exactly one row per postcode."""
    total_rows = risk_zone.count()
    unique_postcodes = risk_zone.select("postcode").distinct().count()
    return {
        "total_rows": total_rows,
        "unique_postcodes": unique_postcodes,
        "is_unique": total_rows == unique_postcodes,
        "extra_rows": total_rows - unique_postcodes,
    }


def business_rule_violations(
    claims: DataFrame,
    events: DataFrame,
    premiums: DataFrame,
) -> dict[str, int]:
    """Run all DQ business-rule counts and return a label -> count map."""
    return {
        "claims_loss_after_reported": count_claims_loss_after_reported(claims),
        "claims_negative_incurred": count_claims_negative_incurred(claims),
        "claims_paid_exceeds_incurred": count_claims_paid_exceeds_incurred(claims),
        "premiums_bad_dates": count_premiums_bad_dates(premiums),
        "premiums_non_positive_sum_insured": count_premiums_non_positive_sum_insured(premiums),
        "premiums_non_positive_annual_premium": count_premiums_non_positive_annual_premium(premiums),
        "events_start_after_end": count_events_start_after_end(events),
    }


def referential_integrity_violations(
    claims: DataFrame,
    events: DataFrame,
    premiums: DataFrame,
    risk_zone: DataFrame,
) -> dict[str, int]:
    """Run all referential integrity orphan counts."""
    return {
        "orphan_claim_policies": count_orphan_claim_policies(claims, premiums),
        "orphan_claim_events": count_orphan_claim_events(claims, events),
        "orphan_premium_postcodes": count_orphan_premium_postcodes(premiums, risk_zone),
    }
