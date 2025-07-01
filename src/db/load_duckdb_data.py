import duckdb
import pandas as pd
import os
import glob
import traceback


def execute_hydration_scripts(db_path, sql_dir):
    """
    Loads processed elk population data from parquet files into a DuckDB database.

    Args:
        processed_data_dir (str): Directory containing processed parquet files.
        db_path (str): The path to the DuckDB database file.
    """

    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    print(sql_files)
    # Connect to DuckDB and write the DataFrame
    with duckdb.connect(database=db_path, read_only=False) as con:
        for sql_file in sql_files:
            with open(sql_file, "r") as f:
                sql_script = f.read()
                print(f"Hydrating using {sql_file}")
            try:
                con.execute(sql_script)
                print(f"Successfull hydrated {sql_file}")
            except Exception as e:
                print(f"Error hydrating: {sql_file}: {e}")

        con.close()


if __name__ == "__main__":
    sql_dir = "./sql/load"
    db_path = "./data/database/herd_data.duckdb"
    execute_hydration_scripts(db_path, sql_dir)
