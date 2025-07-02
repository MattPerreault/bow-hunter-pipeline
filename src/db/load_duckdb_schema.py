import duckdb
import os
import glob


def execute_sql_scripts(db_path, sql_dir):
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*sql")))
    print(sql_files)
    with duckdb.connect(database=db_path) as con:

        for sql_file in sql_files:
            print(f"Executing {sql_file}...")

            with open(sql_file, "r") as f:
                sql_script = f.read()

            try:
                con.execute(sql_script)
                print(f"Successfully executed {sql_file}")
            except Exception as e:
                print(f"Error executing {sql_file}: {e}")
        
        con.close()


if __name__ == "__main__":
    db_path = "./data/database/herd_data.duckdb"
    sql_dir = "./sql/create"
    execute_sql_scripts(db_path, sql_dir)
