"""Leakage-safe feature engineering for ultimate claim severity prediction."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

FEATURE_COLUMNS = [
    "claim_id",
    "policy_id",
    "first_incurred",
    "peril_type",
    "report_lag_days",
    "wind_risk_band",
    "building_type",
    "sum_insured",
    "mitigation_flag",
    "region_name",
    "ultimate_incurred",
]

MODEL_FEATURE_COLUMNS = [
    "first_incurred",
    "peril_type",
    "report_lag_days",
    "wind_risk_band",
    "building_type",
    "sum_insured",
    "mitigation_flag",
    "region_name",
]

CATEGORICAL_COLUMNS = [
    "peril_type",
    "wind_risk_band",
    "building_type",
    "mitigation_flag",
    "region_name",
]

NUMERIC_COLUMNS = [
    "first_incurred",
    "report_lag_days",
    "sum_insured",
]


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def build_claim_severity_features(
    claims: pd.DataFrame,
    policies: pd.DataFrame,
) -> pd.DataFrame:
    """Build one training row per claim that ends Closed with numeric ultimate incurred.

    Uses only the earliest snapshot for predictors (no later paid/incurred leakage).
    """
    required_claim_cols = {
        "claim_id",
        "policy_id",
        "date_of_loss",
        "reported_date",
        "peril_type",
        "claim_status",
        "incurred_amount",
        "snapshot_date",
    }
    missing = required_claim_cols - set(claims.columns)
    if missing:
        raise ValueError(f"claims missing columns: {sorted(missing)}")

    required_policy_cols = {
        "policy_id",
        "wind_risk_band",
        "building_type",
        "sum_insured",
        "mitigation_flag",
        "region_name",
    }
    missing_p = required_policy_cols - set(policies.columns)
    if missing_p:
        raise ValueError(f"policies missing columns: {sorted(missing_p)}")

    c = claims.copy()
    c["incurred_amount"] = _to_numeric(c["incurred_amount"])
    c["snapshot_date"] = _parse_dates(c["snapshot_date"])
    c["date_of_loss"] = _parse_dates(c["date_of_loss"])
    c["reported_date"] = _parse_dates(c["reported_date"])
    c = c.dropna(subset=["claim_id", "snapshot_date", "incurred_amount"])

    latest = (
        c.sort_values(["claim_id", "snapshot_date"])
        .groupby("claim_id", as_index=False)
        .tail(1)
    )
    closed = latest[latest["claim_status"].astype(str).str.strip() == "Closed"].copy()
    closed = closed.rename(columns={"incurred_amount": "ultimate_incurred"})

    first = (
        c.sort_values(["claim_id", "snapshot_date"])
        .groupby("claim_id", as_index=False)
        .head(1)
        .rename(
            columns={
                "incurred_amount": "first_incurred",
                "peril_type": "peril_type",
            }
        )
    )
    first["report_lag_days"] = (
        first["reported_date"] - first["date_of_loss"]
    ).dt.days

    features = closed[
        ["claim_id", "policy_id", "ultimate_incurred"]
    ].merge(
        first[
            [
                "claim_id",
                "first_incurred",
                "peril_type",
                "report_lag_days",
            ]
        ],
        on="claim_id",
        how="inner",
    )

    p = policies.copy()
    p["sum_insured"] = _to_numeric(p["sum_insured"])
    p = p.drop_duplicates(subset=["policy_id"], keep="first")

    features = features.merge(
        p[
            [
                "policy_id",
                "wind_risk_band",
                "building_type",
                "sum_insured",
                "mitigation_flag",
                "region_name",
            ]
        ],
        on="policy_id",
        how="inner",
    )

    features = features.dropna(
        subset=[
            "first_incurred",
            "ultimate_incurred",
            "report_lag_days",
            "sum_insured",
            "peril_type",
            "wind_risk_band",
            "building_type",
            "mitigation_flag",
            "region_name",
        ]
    )

    return features[FEATURE_COLUMNS].reset_index(drop=True)


def model_matrix_columns() -> Iterable[str]:
    return list(MODEL_FEATURE_COLUMNS)
