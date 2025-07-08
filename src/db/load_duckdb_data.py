import duckdb
import pandas as pd
import os
import glob
import traceback


def _configure_duckdb_s3(con):
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    if aws_access_key and aws_secret_key:
        con.execute(f"SET s3_region='{aws_region}';")
        con.execute(f"SET s3_access_key_id='{aws_access_key}';")
        con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")
        con.execute("SET s3_url_style='path';")
        print("DuckDB S3 configuration set.")
    else:
        print("⚠️ AWS credentials not found in environment. S3 reads will fail.")


def execute_hydration_scripts(db_path, sql_dir):
    """
    Loads processed elk population stage data from parquet files into a DuckDB database.

    Args:
        processed_data_dir (str): Directory containing processed parquet files.
        db_path (str): The path to the DuckDB database file.
    """

    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    # Connect to DuckDB and write the DataFrame
    with duckdb.connect(database=db_path, read_only=False) as con:

        _configure_duckdb_s3(con)

        for sql_file in sql_files:
            with open(sql_file, "r") as f:
                sql_script = f.read()
                print(f"Hydrating using {sql_file}")
            try:
                con.execute(sql_script)
                print(f"Successfull hydrated {sql_file}")
            except Exception as e:
                print(f"Error hydrating: {sql_file}: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Hydrate DuckDB tables from S3 parquet using SQL files."
    )
    parser.add_argument(
        "--sql_dir", default="./sql/load", help="Directory containing SQL load scripts."
    )
    parser.add_argument(
        "--db_path",
        default="./data/database/herd_data.duckdb",
        help="Path to DuckDB database.",
    )
    args = parser.parse_args()

    execute_hydration_scripts(args.db_path, args.sql_dir)
