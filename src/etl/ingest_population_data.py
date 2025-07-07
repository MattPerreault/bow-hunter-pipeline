import argparse
import pandas as pd
import re
import os
import time
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

session = boto3.Session(profile_name="default")
s3_client = session.client("s3")
textract = session.client("textract")


STATE_MAP = {
    "ak": "alaska",
    "az": "arizona",
    "ca": "california",
    "co": "colorado",
    "id": "idaho",
    "mt": "montana",
    "nm": "new_mexico",
    "nv": "nevada",
    "or": "oregon",
    "ut": "utah",
    "wa": "washington",
    "wy": "wyoming",
}


def _list_raw_population_pdfs(bucket_name: str, state: str, species: str):
    """
    Lists all raw population PDF files in S3 for a specific state and species.
    """
    prefix = f"raw/{state}/{species}/population/"
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    pdf_files = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".pdf"):
                pdf_files.append(key)

    return pdf_files


def _processed_parquet_exists(
    bucket_name: str, state: str, species: str, year: int, filename_base: str
):
    """
    Checks if the processed Parquet file already exists in S3.
    """
    key = f"processed/{state}/{species}/population/{year}/{filename_base}.parquet"
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        print(f"✅ Processed file already exists: {key}. Skipping reprocessing.")
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            print(f"❌ Error checking processed file: {e}")
            # Optional: TODO SQS DLQ for this error case
            return False


def _process_single_file(
    bucket_name: str, object_key: str, state: str, species: str, year: int
):
    print(f"🚀 Ingesting {species} population data for {state.upper()} {year}")

    job_id = start_textract_pdf_analysis(bucket_name, object_key)
    status = _wait_for_job(job_id)

    if status != "SUCCEEDED":
        print(f"❌ Textract job {job_id} failed for {object_key}. Skipping.")
        # TODO: Add to DLQ for reprocessing later
        sys.exit(1)

    table = ingest_population_data(job_id)
    df = rows_to_data_frame(table, state, species, year)
    write_parquet_to_s3(df, bucket_name, state, species, year, object_key)

    print(f"✅ Successfully processed: {object_key}")


def start_textract_pdf_analysis(bucket: str, object_key: str):
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": object_key}},
        FeatureTypes=["TABLES"],  # only tables, not forms
    )

    job_id = response["JobId"]

    print(f"Textract job id {job_id}")

    return job_id


def _wait_for_job(job_id):
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

    # Maps species to potential male_female_ratio header variants
    SPECIES_RATIO_HEADERS = {
        "elk": ["bull_cow_ratio_(per_100)", "bull_per_cow_ratio_(per_100)"],
        "deer": ["buck_doe_ratio_(per_100)", "buck_per_doe_ratio_(per_100)"],
        "pronghorn": ["buck_per_doe_ratio_(per_100)"],
    }

    # Potential GMU column header regexes (with typo resilience)
    GMU_HEADER_PATTERNS = [
        r"game_management_units_involved_in_\d{4}",  # correct pattern
        r"game_management_unites_involved_in_\d{4}",  # typo resilience
    ]

    # Handle GMU column renaming dynamically if needed
    if "gmu_list" in df.columns:
        print("Column 'gmu_list' already present, no renaming needed.")
    else:
        gmu_col = next(
            (
                col
                for col in df.columns
                for pattern in GMU_HEADER_PATTERNS
                if re.match(pattern, col)
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

    ratio_headers = SPECIES_RATIO_HEADERS.get(species.lower(), [])
    found_ratio_col = None
    possible_ratio_cols = ratio_headers + ["male_female_ratio"]

    for ratio_col in possible_ratio_cols:
        if ratio_col in df.columns:
            found_ratio_col = ratio_col
            break

    if found_ratio_col:
        df["male_female_ratio"] = pd.to_numeric(
            df[found_ratio_col].str.replace(",", ""), errors="coerce"
        )
        if found_ratio_col != "male_female_ratio":
            df = df.drop(columns=[found_ratio_col])
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
    bucket_name = os.environ.get("HERD_KNOWLEDGE_BUCKET")

    if not bucket_name:
        raise EnvironmentError("HERD_KNOWLEDGE_BUCKET env var not set.")

    # Set up CLI args
    parser = argparse.ArgumentParser(
        description="Ingest population data via Textract and store as Parquet in S3."
    )
    parser.add_argument("--state", required=True, help="State abbreviation, e.g., 'co'")
    parser.add_argument(
        "--species", required=True, help="Species, e.g., 'deer' or 'elk'"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=False,
        help="Year of data, e.g., 2019. If omitted, batch processes all available raw files for state/species.",
    )
    args = parser.parse_args()

    state = args.state
    species = args.species
    year = args.year

    full_state = STATE_MAP.get(state.lower())

    if not full_state:
        raise ValueError(f"State '{state}' not supported yet.")

    raw_files = _list_raw_population_pdfs(bucket_name, state, species)

    if year:
        raw_file = f"{full_state}_{species}_population_{year}.pdf"
        object_key = f"raw/{state}/{species}/population/{raw_file}"

        filename_base = os.path.splitext(os.path.basename(raw_file))[0]

        # Check if already processed
        if _processed_parquet_exists(bucket_name, state, species, year, filename_base):
            print(f"✅ Already processed: {object_key}. Skipping...")
        else:
            _process_single_file(bucket_name, object_key, state, species, year)
    else:
        # Batch mode: process all raw files not yet processed.
        for raw_file in raw_files:
            filename_base = os.path.splitext(os.path.basename(raw_file))[0]
            year_match = re.search(r"(\d{4})", raw_file)
            if year_match:
                year_extracted = year_match.group(1)
                if _processed_parquet_exists(
                    bucket_name, state, species, int(year_extracted), filename_base
                ):
                    print(f"✅ Already processed: {raw_file}. Skipping...")
                    continue
                else:
                    _process_single_file(
                        bucket_name, raw_file, state, species, int(year_extracted)
                    )
            else:
                print(f"❌ Could not extract year from filename: {raw_file}")
                # TODO: Send this to DLQ for inspection later
