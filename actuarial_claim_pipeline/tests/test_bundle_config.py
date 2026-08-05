"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_job_task_order():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_claims_job.job.yml").read_text())
    tasks = job["resources"]["jobs"]["actuarial_claims_job"]["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == ["setup", "bronze", "data_quality", "silver", "gold"]

    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    assert deps["bronze"] == ["setup"]
    assert deps["data_quality"] == ["bronze"]
    assert deps["silver"] == ["data_quality"]
    assert deps["gold"] == ["silver"]


def test_dev_and_prod_variable_targets():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["targets"]["dev"]["variables"]["schema"] == "dev"
    assert bundle["targets"]["dev"]["variables"]["overwrite_schema"] == "true"
    assert bundle["targets"]["prod"]["variables"]["schema"] == "prod"
    assert bundle["targets"]["prod"]["variables"]["overwrite_schema"] == "false"


def test_job_environment_includes_wheel():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_claims_job.job.yml").read_text())
    env = job["resources"]["jobs"]["actuarial_claims_job"]["environments"][0]
    assert env["environment_key"] == "default"
    assert any("*.whl" in dep for dep in env["spec"]["dependencies"])
