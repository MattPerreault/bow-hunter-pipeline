import glob
import pandas as pd
import re
import os
import time
from collections import defaultdict

import json

import boto3
from botocore.exceptions import ClientError

session = boto3.Session(profile_name="default")
s3_client = session.client("s3")
textract = session.client("textract")


def start_textract_pdf_analysis(bucket: str, object_key: str):
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": object_key}},
        FeatureTypes=["TABLES"],  # only tables, not forms
    )

    job_id = response["JobId"]

    print(f"Textract job id {job_id}")

    return job_id


def wait_for_job(job_id):
    while True:
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp["JobStatus"]

        if status in ["SUCCEEDED", "FAILED"]:
            return status
        time.sleep(10)


def ingest_population_data(job_id: str):
    """
    Ingests elk population and sex ratio post-hunt data from a PDF, processes it, and write it to a parquet file
    to be loaded into a S3 lake.

    Args:
        blocks (list): list of block objects from parsed PDF
        state (str): State abreviation
        species (str): Population species
        year (int): The year of the data.
    """

    resp = textract.get_document_analysis(JobId=job_id)
    blocks = resp["Blocks"]

    # Create word map for easy lookup
    word_map = {
        block["Id"]: block["Text"] for block in blocks if block["BlockType"] == "WORD"
    }

    cell_blocks = [block for block in blocks if block["BlockType"] == "CELL"]

    # Create a grid: {row_index: {col_index: cell_data}}
    table_grid = defaultdict(dict)

    for block in cell_blocks:
        row_index = block["RowIndex"]
        col_index = block["ColumnIndex"]

        # Retrieve child word IDs from CELL block relationships
        cell_data = []
        for relationship in block.get("Relationships", []):
            if relationship["Type"] == "CHILD":
                for word_id in relationship["Ids"]:
                    if word_id in word_map:
                        cell_data.append(word_map[word_id])

        cell_text = " ".join(cell_data).strip()
        table_grid[row_index][col_index] = cell_text

    # Convert to a list of listsclear
    max_col = max(col for row in table_grid.values() for col in row)

    table = []
    for row_index in sorted(table_grid.keys()):
        row = [
            table_grid[row_index].get(col_index, "")
            for col_index in range(1, max_col + 1)
        ]
        table.append(row)
    return table


def rows_to_data_frame(
    table: list, state: str, species: str, year: int
) -> pd.DataFrame:
    """
    Converts parsed table rows into a standardized DataFrame matching the 'population' table schema.

    Handles:
    - Dynamic GMU column naming
    - herd_name fallback if missing
    - Parsing numeric columns
    - Adds state, species, year metadata
    - Matches schema:
        state, species, herd_name, post_hunt_estimate, male_female_ratio, year, gmu_list
    """

    raw_headers = table[0]
    print(f"Raw headers: {raw_headers}")

    # Clean headers consistently
    headers = [h.lower().replace(" ", "_").replace("/", "_per_") for h in raw_headers]
    print(f"Cleaned headers: {headers}")

    data_rows = table[1:]

    # Filter out footer rows like 'Total'
    filtered_rows = [row for row in data_rows if row[0].strip().lower() != "total"]

    # Build DataFrame
    df = pd.DataFrame(filtered_rows, columns=headers)

    # Add metadata columns
    df["state"] = state
    df["species"] = species
    df["year"] = year

    # Handle GMU column renaming dynamically if needed
    if "gmu_list" in df.columns:
        print("Column 'gmu_list' already present, no renaming needed.")
    else:
        gmu_col = next(
            (
                col
                for col in df.columns
                if re.match(r"game_management_units_involved_in_\d{4}", col)
            ),
            None,
        )
        if gmu_col:
            print(f"Renaming column {gmu_col} to 'gmu_list' for consistency.")
            df = df.rename(columns={gmu_col: "gmu_list"})
        else:
            print(
                "No GMU column found matching expected pattern; please check column headers."
            )

    # Parse numeric columns robustly
    if "post_hunt_estimate" in df.columns:
        df["post_hunt_estimate"] = pd.to_numeric(
            df["post_hunt_estimate"].str.replace(",", ""), errors="coerce"
        )
    else:
        print("Warning: 'post_hunt_estimate' column missing.")

    if "buck_doe_ratio_(per_100)" in df.columns:
        df["male_female_ratio"] = pd.to_numeric(
            df["buck_doe_ratio_(per_100)"].str.replace(",", ""), errors="coerce"
        )
        df = df.drop(columns=["buck_doe_ratio_(per_100)"])
    elif "bull_cow_ratio_(per_100)" in df.columns:
        df["male_female_ratio"] = pd.to_numeric(
            df["bull_cow_ratio_(per_100)"].str.replace(",", ""), errors="coerce"
        )
        df = df.drop(columns=["bull_cow_ratio_(per_100)"])
    elif "male_female_ratio" in df.columns:
        df["male_female_ratio"] = pd.to_numeric(
            df["male_female_ratio"].str.replace(",", ""), errors="coerce"
        )
    else:
        print(
            "Warning: No male:female ratio column found; 'male_female_ratio' will be NaN."
        )

    # Handle herd_name availability or fallback to DAU
    if "herd_name" in df.columns:
        print("Using herd_name from PDF data.")
    else:
        print("herd_name not present; defaulting to 'DAU_<dau*>' naming.")
        if "dau*" in df.columns:
            df = df.rename(columns={"dau*": "dau"})
            df["herd_name"] = "DAU_" + df["dau"].astype(str)
        else:
            print("Warning: 'dau*' column not found; 'herd_name' will be missing.")

    return df


def write_parquet_to_s3(
    df: pd.DataFrame, bucket_name: str, state: str, species: str, year: int, key: str
):
    """
    Writes the DataFrame as Parquet to S3 under the structured processed data path.
    """

    # Construct destination path
    source_basename = os.path.splitext(os.path.basename(key))[0]
    s3_path = f"s3://{bucket_name}/processed/{state}/{species}/population/{year}/{source_basename}.parquet"

    print(f"Writing parquet to: {s3_path}")

    # Write Parquet to S3
    df.to_parquet(s3_path, index=False, storage_options={"profile": "default"})

    print(f"Successfully wrote parquet to {s3_path}")


if __name__ == "__main__":
    bucket_name = "grand-lake"
    object_key = "raw/co/deer/population/colorado_deer_population_2019.pdf"

    # Start textract Job
    job_id = start_textract_pdf_analysis(bucket_name, object_key)
    wait_for_job(job_id)

    table = ingest_population_data(job_id)

    df = rows_to_data_frame(table, "co", "deer", 2019)

    write_parquet_to_s3(df, bucket_name, "co", "deer", 2019, object_key)
