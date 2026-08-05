"""Validate prepared sample-data fixtures."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures" / "sample-data"
UPSTREAM_CLAIMS = ROOT.parent / "sample-data" / "claims_bordereau.csv"


def _header(path: Path) -> str:
    return path.read_text().splitlines()[0]


def _data_row_count(path: Path) -> int:
    lines = path.read_text().splitlines()
    return max(0, len(lines) - 1)


def test_claims_batches_exist_with_matching_headers():
    batches = [
        FIXTURES / "claims" / "claims_batch_01.csv",
        FIXTURES / "claims" / "claims_batch_02.csv",
        FIXTURES / "claims" / "claims_batch_03.csv",
    ]
    for path in batches:
        assert path.is_file(), f"missing {path}"

    headers = {_header(p) for p in batches}
    assert len(headers) == 1
    if UPSTREAM_CLAIMS.is_file():
        assert headers.pop() == _header(UPSTREAM_CLAIMS)


def test_claims_batch_rows_sum_to_expected():
    batches = [
        FIXTURES / "claims" / "claims_batch_01.csv",
        FIXTURES / "claims" / "claims_batch_02.csv",
        FIXTURES / "claims" / "claims_batch_03.csv",
    ]
    total = sum(_data_row_count(p) for p in batches)
    assert total == 2832
    if UPSTREAM_CLAIMS.is_file():
        assert total == _data_row_count(UPSTREAM_CLAIMS)


def test_dimension_files_present():
    assert (FIXTURES / "premiums" / "premium_bordereau.csv").is_file()
    assert (FIXTURES / "risk_zones" / "risk_zone_lookup.csv").is_file()
    assert (FIXTURES / "cyclone_events" / "cyclone_events.csv").is_file()
