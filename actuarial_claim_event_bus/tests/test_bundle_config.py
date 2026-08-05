"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_name_and_event_bus_schema():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "actuarial_claim_event_bus"
    for target in ("dev", "prod"):
        vars_ = bundle["targets"][target]["variables"]
        assert vars_["schema"] == "event_bus"
        assert vars_["catalog"] == "actuarial"
        assert "servicebus.windows.net" in vars_["eh_bootstrap_servers"]
        assert "secrets/actuarial-event-bus/eh-jaas" in vars_["eh_jaas_config"]


def test_pipeline_is_serverless_continuous():
    pipeline = yaml.safe_load(
        (ROOT / "resources" / "actuarial_claim_event_bus_etl.pipeline.yml").read_text()
    )
    cfg = pipeline["resources"]["pipelines"]["actuarial_claim_event_bus_etl"]
    assert cfg["serverless"] is True
    assert cfg["continuous"] is True
    assert cfg["schema"] == "event_bus"
    assert cfg["configuration"]["eh_bootstrap_servers"] == "${var.eh_bootstrap_servers}"
    assert cfg["configuration"]["eh_jaas_config"] == "${var.eh_jaas_config}"


def test_job_task_order():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_event_bus_job.job.yml").read_text())
    job_cfg = job["resources"]["jobs"]["actuarial_event_bus_job"]
    tasks = job_cfg["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == ["setup", "seed_event_hubs", "start_pipeline"]

    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    assert deps["seed_event_hubs"] == ["setup"]
    assert deps["start_pipeline"] == ["seed_event_hubs"]

    start = next(t for t in tasks if t["task_key"] == "start_pipeline")
    assert "pipeline_id" in start["pipeline_task"]


def test_schema_resource_targets_event_bus():
    schema = yaml.safe_load((ROOT / "resources" / "event_bus.schema.yml").read_text())
    sch = schema["resources"]["schemas"]["actuarial_event_bus"]
    assert sch["name"] == "event_bus"


def test_transform_modules_cover_bronze_only():
    tx = ROOT / "src" / "actuarial_claim_event_bus_etl" / "transformations"
    expected = [
        "bronze_claims_bordereau_raw.py",
        "bronze_claims_bordereau.py",
        "quarantine_bronze_claims_bordereau.py",
        "bronze_premium_bordereau_raw.py",
        "bronze_premium_bordereau.py",
        "quarantine_bronze_premium_bordereau.py",
        "bronze_risk_zone_lookup_raw.py",
        "bronze_risk_zone_lookup.py",
        "quarantine_bronze_risk_zone_lookup.py",
        "bronze_cyclone_events_raw.py",
        "bronze_cyclone_events.py",
        "quarantine_bronze_cyclone_events.py",
    ]
    for name in expected:
        assert (tx / name).is_file(), f"missing transform {name}"

    # Bronze-only: no silver/gold/temp-view modules.
    for path in tx.glob("*.py"):
        assert not path.name.startswith("silver_")
        assert not path.name.startswith("gold_")
        assert not path.name.startswith("v_")


def test_ci_workflow_exists():
    wf = ROOT.parent / ".github" / "workflows" / "actuarial-claim-event-bus.yml"
    assert wf.is_file()
    text = wf.read_text()
    assert "actuarial_claim_event_bus" in text
    assert "test_smoke_integration.py" in text


def test_fixture_jsonl_exists():
    fixtures = ROOT / "fixtures" / "sample-events"
    for subdir in ("claims", "premiums", "risk_zones", "cyclone_events"):
        files = list((fixtures / subdir).glob("*.jsonl"))
        assert files, f"missing jsonl under {subdir}"
        # At least one non-empty line
        lines = [ln for ln in files[0].read_text().splitlines() if ln.strip()]
        assert lines, f"empty fixture {files[0]}"
