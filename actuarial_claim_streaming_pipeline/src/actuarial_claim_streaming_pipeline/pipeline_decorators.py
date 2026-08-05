"""Resolve Lakeflow dataset decorators across runtime versions."""

from pyspark import pipelines as dp

# Temporary views: newer runtimes expose temporary_view; older / dlt-compat use view.
temporary_view = getattr(dp, "temporary_view", dp.view)

# Materialized views: prefer explicit materialized_view; batch @dp.table is the fallback.
materialized_view = getattr(dp, "materialized_view", dp.table)
