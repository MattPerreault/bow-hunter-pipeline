import duckdb
import pandas as pd
import os
import glob
import traceback


def load_elk_population_to_duckdb(
    processed_data_dir: str, db_path: str = "./data/database/herd_data.duckdb"
):
    """
    Loads processed elk population data from parquet files into a DuckDB database.

    Args:
        processed_data_dir (str): Directory containing processed parquet files.
        db_path (str): The path to the DuckDB database file.
    """
    if not os.path.exists(processed_data_dir):
        print(f"Error: Processed data directory not found at {processed_data_dir}")
        return

    # Ensure the database directory exists
    db_dir = os.path.dirname(db_path)
    os.makedirs(db_dir, exist_ok=True)

    parquet_files = glob.glob(
        os.path.join(processed_data_dir, "**", "*.parquet"), recursive=True
    )
    if not parquet_files:
        print(f"No parquet files found in {processed_data_dir}")
        return

    all_df = pd.DataFrame()
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            all_df = pd.concat([all_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error reading parquet file {parquet_file}: {e}")
            traceback.print_exc()
            continue

    if all_df.empty:
        print("No data to load into DuckDB.")
        return

    # Connect to DuckDB and write the DataFrame
    try:
        with duckdb.connect(database=db_path, read_only=False) as con:
            table_name = "elk_population"

            # Drop the table if it exists to easily change primary key, then recreate
            con.execute(f"DROP TABLE IF EXISTS {table_name};")

            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    dau VARCHAR,
                    herd_name VARCHAR,
                    gmu_list VARCHAR,
                    post_hunt_estimate BIGINT,
                    bull_cow_ratio DOUBLE,
                    year BIGINT,
                    PRIMARY KEY (herd_name, year)
                );
            """
            )

            # Register the DataFrame as a temporary view for table upsert
            con.register("new_elk_data", all_df)

            con.execute(
                f"""
                INSERT INTO {table_name} 
                SELECT dau, herd_name, gmu_list, post_hunt_estimate, bull_cow_ratio, year 
                FROM new_elk_data
                ON CONFLICT (herd_name, year) 
                DO UPDATE SET
                    dau = EXCLUDED.dau,
                    gmu_list = EXCLUDED.gmu_list,
                    post_hunt_estimate = EXCLUDED.post_hunt_estimate,
                    bull_cow_ratio = EXCLUDED.bull_cow_ratio;
            """
            )

        con.close()
        print(
            f"Data successfully upserted into DuckDB table '{table_name}' from parquet files at {db_path}"
        )
    except Exception as e:
        print(f"Error loading data to DuckDB: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    processed_dir = "./data/processed/elk/population"
    duckdb_database_path = "./data/database/herd_data.duckdb"
    load_elk_population_to_duckdb(processed_dir, duckdb_database_path)
