import argparse

from databricks.sdk.runtime import spark


def main():
    parser = argparse.ArgumentParser(
        description="Event bus pipeline helper entrypoint (catalog parameter).",
    )
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql("SHOW SCHEMAS").show()


if __name__ == "__main__":
    main()
