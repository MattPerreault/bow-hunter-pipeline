import re
import pdfplumber
import pandas as pd
import duckdb
import os
import glob

def ingest_elk_population_data(pdf_path: str, year: int, db_path: str = './data/database/herd_data.duckdb'):
    """
    Ingests elk population and sex ratio post-hunt data from a PDF, processes it, and write it to a parquet file
    to be loaded into a DuckDB database.

    Args:
        pdf_path (str): The path to the input PDF file.
        year (int): The year of the data.
        db_path (str): The path to the DuckDB database file.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return

    # Ensure the database directory exists
    db_dir = os.path.dirname(db_path)
    os.makedirs(db_dir, exist_ok=True)

    data = []
    try:
        # Parse PDF
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            tables = page.extract_tables()

            if not tables:
                print("No tables found in the PDF.")
                return
            
            table = tables[0]

            print(table)
            for row in table[1:]:
                row_text = str(row[0]).strip()
                
                if any(keyword in row_text for keyword in ['Total', 'DAU (', '* DAU']):
                    continue

                # Parse the row data using known DAU structure (first 2 digits)
                dau_match = re.match(r'^(\d{2})', row_text)
                if not dau_match:
                    print(f"Could not find DAU in row: {row_text}")
                    continue

                dau = dau_match.group(1)

                # Remove DAU from the beginning and split the rest
                remaining_text = row_text[2:].strip().split()

                # Need at least herd_name, gmu, estimate, ratio
                if len(remaining_text) >= 4: 
                    ratio = remaining_text[-1]
                    estimate = remaining_text[-2]

                    # Separate herd name from GMU
                    name_and_gmu_parts = remaining_text[:-2]

                    # Parse out herd name and GMU
                    # GMU is a list of ints
                    for i, part in enumerate(name_and_gmu_parts):
                        if re.match(r'^\d+', part) or ',' in part:
                            gmu_start_idx = i
                            break
                    
                    if gmu_start_idx is not None:
                        herd_name_parts = name_and_gmu_parts[:gmu_start_idx]
                        gmu_parts = name_and_gmu_parts[gmu_start_idx:]
                    else:
                        # Assume last part before numbers is GMU
                        herd_name_parts = name_and_gmu_parts[:-1]
                        gmu_parts = [name_and_gmu_parts[-1]] if name_and_gmu_parts else []

                    herd_name = ' '.join(herd_name_parts)
                    gmu = ''.join(gmu_parts)

                    data.append([dau, herd_name, gmu, estimate, ratio, year])

                else:
                    print(f"Could not parse row (insufficient parts): {remaining_text}")
                
        df = pd.DataFrame(data, columns=['dau', 'herd_name', 'gmu_list', 'post_hunt_estimate', 'bull_cow_ratio', 'year'])

        # Clean datatypes
        df['dau'] = df['dau'].astype(str)
        df['herd_name'] = df['herd_name'].astype(str)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        df['post_hunt_estimate'] = pd.to_numeric(
            df['post_hunt_estimate'].str.replace(',',''),
            errors='coerce'
        )

        df['bull_cow_ratio'] = pd.to_numeric(
            df['bull_cow_ratio'],
            errors='coerce'
        )

        # Drop empty herds
        df = df[df['bull_cow_ratio'] != 0]

        # Create and write processed data to parquet file  
        parquet_dir = f'./data/processed/elk/population/{year}'
        os.makedirs(parquet_dir, exist_ok=True)
        parquet_file_path = os.path.join(parquet_dir, f'colorado_elk_population_{year}.parquet')

        df.to_parquet(parquet_file_path, index=False)

    except ValueError as e:
        print(f"Error creating DataFrame: {e}. Check headers and data rows consistency.")
        return

    # Connect to DuckDB and write the DataFrame
    try:
        with duckdb.connect(database=db_path, read_only=False) as con:
            table_name = "elk_population"

            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    dau VARCHAR,
                    herd_name VARCHAR,
                    gmu_list VARCHAR,
                    post_hunt_estimate BIGINT,
                    bull_cow_ratio DOUBLE,
                    year BIGINT,
                    PRIMARY KEY (dau, herd_name, gmu_list, year)
                );
            """)

            # Register the DataFrame as a temporary view for table upsert
            con.register("new_elk_data", df)

            con.execute(f"""
                INSERT INTO {table_name} 
                SELECT dau, herd_name, gmu_list, post_hunt_estimate, bull_cow_ratio, year 
                FROM new_elk_data
                ON CONFLICT (dau, herd_name, gmu_list, year) 
                DO UPDATE SET
                    post_hunt_estimate = EXCLUDED.post_hunt_estimate,
                    bull_cow_ratio = EXCLUDED.bull_cow_ratio;
            """) 
        
        con.close()
        print(f"Data successfully upserted into DuckDB table '{table_name}' from parquet at {db_path}")
    except Exception as e:
        print(f"Error loading data to DuckDB: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    duckdb_database_path = './data/database/herd_data.duckdb'

    pdf_files = glob.glob('./data/raw/elk/population/*.pdf')
    for pdf_file_path in pdf_files:
        match = re.search(r'(\d{4})\.pdf$', pdf_file_path)
        if match:
            year = int(match.group(1))
            print(f"Ingesting data for year: {year} from {pdf_file_path}")
            ingest_elk_population_data(pdf_file_path, year, duckdb_database_path)
        else:
            print(f"Could not extract year from filename: {pdf_file_path}") 