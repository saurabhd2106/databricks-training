"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_name_and_landing_path():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "actuarial_claim_streaming_pipeline"
    for target in ("dev", "prod"):
        landing = bundle["targets"][target]["variables"]["landing_volume_path"]
        assert "streaming" in landing
        assert landing.endswith("/landing")


def test_pipeline_is_serverless_triggered():
    pipeline = yaml.safe_load(
        (ROOT / "resources" / "actuarial_claim_streaming_etl.pipeline.yml").read_text()
    )
    cfg = pipeline["resources"]["pipelines"]["actuarial_claim_streaming_etl"]
    assert cfg["serverless"] is True
    assert cfg.get("continuous") in (None, False)
    assert cfg["schema"] == "streaming"
    assert cfg["configuration"]["landing_path"] == "${var.landing_volume_path}"


def test_job_task_order_and_claims_batch_default():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_streaming_job.job.yml").read_text())
    job_cfg = job["resources"]["jobs"]["actuarial_streaming_job"]
    tasks = job_cfg["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == ["setup", "land_sample_data", "refresh_pipeline"]

    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    assert deps["land_sample_data"] == ["setup"]
    assert deps["refresh_pipeline"] == ["land_sample_data"]

    params = {p["name"]: p["default"] for p in job_cfg["parameters"]}
    assert params["claims_batch"] == "01"
    assert "streaming" in params["landing_path"] or params["landing_path"] == "${var.landing_volume_path}"

    refresh = next(t for t in tasks if t["task_key"] == "refresh_pipeline")
    assert "pipeline_id" in refresh["pipeline_task"]
    assert refresh["pipeline_task"].get("full_refresh") in (None, False)


def test_volume_targets_streaming_schema():
    volume = yaml.safe_load((ROOT / "resources" / "landing.volume.yml").read_text())
    vol = volume["resources"]["volumes"]["actuarial_streaming_landing"]
    assert vol["schema_name"] == "streaming"
    assert vol["name"] == "landing"


def test_transform_modules_cover_medallion_and_quarantine():
    tx = ROOT / "src" / "actuarial_claim_streaming_etl" / "transformations"
    expected = [
        "bronze_claims_bordereau_raw.py",
        "bronze_claims_bordereau.py",
        "quarantine_bronze_claims_bordereau.py",
        "v_claims_typed.py",
        "v_premiums_typed.py",
        "silver_claims_bordereau.py",
        "silver_claims_current.py",
        "silver_premium_bordereau.py",
        "silver_cyclone_events.py",
        "silver_risk_zone_lookup.py",
        "gold_claims_summary.py",
        "gold_loss_ratio_by_risk.py",
        "gold_event_loss_summary.py",
        "gold_portfolio_exposure.py",
        "gold_claims_development.py",
    ]
    for name in expected:
        assert (tx / name).is_file(), f"missing transform {name}"


def test_ci_workflow_exists():
    wf = ROOT.parent / ".github" / "workflows" / "actuarial-claim-streaming-pipeline.yml"
    assert wf.is_file()
    text = wf.read_text()
    assert "actuarial_claim_streaming_pipeline" in text
    assert "actuarial_streaming_job" in text
    assert "test_smoke_integration.py" in text
