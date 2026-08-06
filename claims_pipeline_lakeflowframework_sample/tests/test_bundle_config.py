"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_name_and_warehouse_id():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "claims_pipeline_lakeflowframework_sample"
    assert "warehouse_id" in bundle["variables"]
    for target in ("dev", "prod"):
        assert bundle["targets"][target]["variables"]["warehouse_id"]
        vars_ = bundle["targets"][target]["variables"]
        assert vars_["catalog"] == "actuarial"
        assert vars_["bronze_schema"] == "sample"
        assert vars_["silver_schema"] == "sample"
        assert vars_["gold_schema"] == "sample"
        assert vars_["landing_volume_path"] == "/Volumes/actuarial/sample/landing"


def test_pipelines_publish_event_logs_to_gold():
    expected = {
        "claims_bronze_pipeline.yml": ("claims_bronze_pipeline", "claims_lff_bronze_event_log"),
        "claims_silver_pipeline.yml": ("claims_silver_pipeline", "claims_lff_silver_event_log"),
        "claims_gold_pipeline.yml": ("claims_gold_pipeline", "claims_lff_gold_event_log"),
    }
    for filename, (resource_key, event_log_name) in expected.items():
        pipeline = yaml.safe_load((ROOT / "resources" / filename).read_text())
        cfg = pipeline["resources"]["pipelines"][resource_key]
        assert cfg["serverless"] is True
        event_log = cfg["event_log"]
        assert event_log["name"] == event_log_name
        assert event_log["catalog"] == "${var.catalog}"
        assert event_log["schema"] == "${var.gold_schema}"


def test_gold_mart_sql_files_exist():
    dml = ROOT / "src" / "dataflows" / "gold" / "dml"
    expected = [
        "event_loss_summary.sql",
        "policy_loss_ratio.sql",
        "risk_band_performance.sql",
        "claims_summary.sql",
        "claims_development.sql",
        "portfolio_exposure.sql",
        "medallion_inventory.sql",
    ]
    for name in expected:
        assert (dml / name).is_file(), f"missing gold SQL {name}"

    spec = yaml.safe_load(
        (ROOT / "src" / "dataflows" / "gold" / "dataflowspec" / "gold_marts_main.yaml").read_text()
    )
    for mart in (
        "event_loss_summary",
        "policy_loss_ratio",
        "risk_band_performance",
        "claims_summary",
        "claims_development",
        "portfolio_exposure",
        "medallion_inventory",
    ):
        assert mart in spec["materializedViews"]


def test_gold_dashboards_and_warehouse_id():
    expected = {
        "underwriting_portfolio": "underwriting_portfolio.lvdash.json",
        "claims_operations": "claims_operations.lvdash.json",
        "catastrophe_events": "catastrophe_events.lvdash.json",
        "claims_development": "claims_development.lvdash.json",
    }
    for resource_key, json_name in expected.items():
        yml = yaml.safe_load((ROOT / "resources" / f"{resource_key}.dashboard.yml").read_text())
        dash = yml["resources"]["dashboards"][resource_key]
        assert dash["warehouse_id"] == "${var.warehouse_id}"
        assert dash["dataset_catalog"] == "${var.catalog}"
        assert dash["dataset_schema"] == "${var.gold_schema}"
        path = ROOT / "src" / "dashboards" / json_name
        assert path.is_file()
        payload = path.read_text()
        assert "actuarial.sample." not in payload
        assert "actuarial." not in payload


def test_pipeline_monitoring_dashboard():
    yml = yaml.safe_load((ROOT / "resources" / "pipeline_monitoring.dashboard.yml").read_text())
    dash = yml["resources"]["dashboards"]["pipeline_monitoring"]
    assert dash["warehouse_id"] == "${var.warehouse_id}"
    assert dash["display_name"] == "Pipeline Monitoring"
    assert dash["dataset_catalog"] == "${var.catalog}"
    assert dash["dataset_schema"] == "${var.gold_schema}"

    path = ROOT / "src" / "dashboards" / "pipeline_monitoring.lvdash.json"
    assert path.is_file()
    payload = path.read_text()
    assert "actuarial." not in payload
    for needle in ("medallion_inventory", "medallion_counts", "ingest_age_hours"):
        assert needle in payload, f"missing {needle} in pipeline monitoring dashboard"


def test_pipeline_event_log_dashboard():
    yml = yaml.safe_load((ROOT / "resources" / "pipeline_event_log.dashboard.yml").read_text())
    dash = yml["resources"]["dashboards"]["pipeline_event_log"]
    assert dash["warehouse_id"] == "${var.warehouse_id}"
    assert dash["display_name"] == "Pipeline Event Log"
    assert dash["dataset_catalog"] == "${var.catalog}"
    assert dash["dataset_schema"] == "${var.gold_schema}"

    path = ROOT / "src" / "dashboards" / "pipeline_event_log.lvdash.json"
    assert path.is_file()
    payload = path.read_text()
    assert "actuarial." not in payload
    for needle in (
        "claims_lff_bronze_event_log",
        "claims_lff_silver_event_log",
        "claims_lff_gold_event_log",
        "update_runs",
        "flow_metrics",
        "expectation_metrics",
        "error_events",
    ):
        assert needle in payload, f"missing {needle} in pipeline event log dashboard"


def test_business_dashboard_queries_use_gold_tables():
    expected_needles = {
        "catastrophe_events.lvdash.json": ("event_loss_summary",),
        "claims_operations.lvdash.json": ("claims_summary",),
        "claims_development.lvdash.json": ("claims_development",),
        "underwriting_portfolio.lvdash.json": (
            "policy_loss_ratio",
            "portfolio_exposure",
            "risk_band_performance",
        ),
    }
    for json_name, needles in expected_needles.items():
        payload = (ROOT / "src" / "dashboards" / json_name).read_text()
        for needle in needles:
            assert needle in payload, f"missing {needle} in {json_name}"


def test_data_quality_expectations_wired():
    """Bronze + silver SCD1 flows enable DQE and point at existing YAML files."""
    contracts = {
        "bronze": {
            "claims_bordereau_main.yaml": (
                "./claims_bordereau_dqe.yaml",
                ("claim_id_not_null", "policy_id_not_null", "snapshot_date_not_null"),
            ),
            "premium_bordereau_main.yaml": (
                "./premium_bordereau_dqe.yaml",
                ("policy_id_not_null", "sum_insured_not_empty"),
            ),
            "cyclone_events_main.yaml": (
                "./cyclone_events_dqe.yaml",
                ("event_id_not_null", "start_date_not_empty"),
            ),
            "risk_zone_lookup_main.yaml": (
                "./risk_zone_lookup_dqe.yaml",
                ("postcode_not_null", "wind_risk_band_not_empty"),
            ),
        },
        "silver": {
            "claims_snapshots_main.yaml": (
                "./claims_snapshots_dqe.yaml",
                (
                    "claim_id_not_null",
                    "policy_id_not_null",
                    "incurred_gte_paid",
                    "reported_on_or_after_loss",
                    "valid_claim_status",
                    "valid_peril_type",
                ),
            ),
            "policies_main.yaml": (
                "./policies_dqe.yaml",
                (
                    "policy_id_not_null",
                    "positive_sum_insured",
                    "positive_annual_premium",
                    "policy_start_before_end",
                    "valid_wind_risk_band",
                ),
            ),
            "cyclone_events_main.yaml": (
                "./cyclone_events_dqe.yaml",
                ("event_id_not_null", "start_on_or_before_end", "dates_not_null"),
            ),
            "risk_zones_main.yaml": (
                "./risk_zones_dqe.yaml",
                ("postcode_not_null", "valid_wind_risk_band"),
            ),
        },
    }

    for layer, specs in contracts.items():
        for spec_name, (dqe_path, rule_names) in specs.items():
            spec_file = ROOT / "src" / "dataflows" / layer / "dataflowspec" / spec_name
            spec = yaml.safe_load(spec_file.read_text())
            assert spec.get("dataQualityExpectationsEnabled") is True, spec_name
            assert spec.get("dataQualityExpectationsPath") == dqe_path, spec_name

            dqe_file = (
                ROOT / "src" / "dataflows" / layer / "expectations" / Path(dqe_path).name
            )
            assert dqe_file.is_file(), f"missing {dqe_file}"
            dqe = yaml.safe_load(dqe_file.read_text())
            names = {
                rule["name"]
                for section in ("expect_or_drop", "expect", "expect_or_fail")
                for rule in dqe.get(section) or []
            }
            for rule_name in rule_names:
                assert rule_name in names, f"{dqe_file.name} missing rule {rule_name}"
