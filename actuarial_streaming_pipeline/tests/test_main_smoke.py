from actuarial_streaming_pipeline.main import main, parse_args


def test_parse_args():
    args = parse_args(["--catalog", "actuarial", "--schema", "streaming"])
    assert args.catalog == "actuarial"
    assert args.schema == "streaming"


def test_main_prints_catalog_schema(capsys):
    main(["--catalog", "actuarial", "--schema", "streaming"])
    captured = capsys.readouterr()
    assert "catalog=actuarial" in captured.out
    assert "schema=streaming" in captured.out
