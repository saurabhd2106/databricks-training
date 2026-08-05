"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_name_and_adls_paths():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "actuarial_ingestion_adls"
    for target in ("dev", "prod"):
        vars_ = bundle["targets"][target]["variables"]
        assert vars_["schema"] == "ingestion"
        assert vars_["landing_path"].startswith("abfss://")
        assert vars_["landing_path"].endswith("/actuarial/ingestion/landing")
        assert vars_["autoloader_state_path"].startswith("abfss://")
        assert vars_["autoloader_state_path"].endswith("/actuarial/ingestion/_autoloader")


def test_job_task_order_and_wheel():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_ingestion_job.job.yml").read_text())
    job_cfg = job["resources"]["jobs"]["actuarial_ingestion_job"]
    tasks = job_cfg["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == ["setup", "land_sample_data", "bronze_ingest"]

    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    assert deps["land_sample_data"] == ["setup"]
    assert deps["bronze_ingest"] == ["land_sample_data"]

    params = {p["name"]: p["default"] for p in job_cfg["parameters"]}
    assert params["claims_batch"] == "01"
    assert params["landing_path"] == "${var.landing_path}"
    assert params["autoloader_state_path"] == "${var.autoloader_state_path}"

    setup = next(t for t in tasks if t["task_key"] == "setup")
    land = next(t for t in tasks if t["task_key"] == "land_sample_data")
    bronze = next(t for t in tasks if t["task_key"] == "bronze_ingest")
    assert "existing_cluster_id" in setup
    assert "existing_cluster_id" in land
    assert bronze.get("environment_key") == "default"

    env = job_cfg["environments"][0]
    assert any("*.whl" in dep for dep in env["spec"]["dependencies"])


def test_schema_resource_targets_ingestion():
    schema = yaml.safe_load((ROOT / "resources" / "ingestion.schema.yml").read_text())
    cfg = schema["resources"]["schemas"]["actuarial_ingestion"]
    assert cfg["name"] == "ingestion"
    assert cfg["catalog_name"] == "${var.catalog}"


def test_ci_workflow_exists():
    wf = ROOT.parent / ".github" / "workflows" / "actuarial-ingestion-adls.yml"
    assert wf.is_file()
    text = wf.read_text()
    assert "actuarial-ingestion-adls" in text
    assert "actuarial_ingestion_job" in text
