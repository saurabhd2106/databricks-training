"""Pytest configuration: optional Databricks Connect Spark session."""

from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture()
def spark():
    """Provide a SparkSession via Databricks Connect, or skip if unavailable."""
    try:
        from databricks.connect import DatabricksSession
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        pytest.skip(f"Databricks Connect not installed: {exc}")

    try:
        conf = WorkspaceClient().config
        if not (
            conf.serverless_compute_id
            or conf.cluster_id
            or os.environ.get("SPARK_REMOTE")
            or os.environ.get("DATABRICKS_HOST")
        ):
            pytest.skip("No Databricks compute / host configured for Spark tests")
        if not (conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE")):
            print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
            os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001 — connection/auth errors vary by SDK
        pytest.skip(f"Unable to create Databricks Spark session: {exc}")
