import argparse


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Actuarial streaming pipeline helper entrypoint.",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    # Job/pipeline entrypoints can use catalog/schema once resources are added.
    print(f"catalog={args.catalog} schema={args.schema}")


if __name__ == "__main__":
    main()
