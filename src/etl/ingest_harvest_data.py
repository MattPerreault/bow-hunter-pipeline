import argparse
import boto3
import os
import pandas as pd
import re
import time

from collections import defaultdict
from typing import List, Dict, Any

# TODO: Trigger on S3 put.
s3_bucket = "grand-lake"
s3_key = "raw/elk/co/harvest/archery/colorado_archery_elk_harvest_2024.pdf"


session = boto3.Session(profile_name="default")
textract = session.client("textract")
s3 = session.client("s3")

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

SPECIES_HARVEST_COLUMN_MAP = {
    "elk": {"bulls": "adult_male", "cows": "adult_female", "calves": "young"},
    "deer": {"bucks": "adult_male", "does": "adult_female", "fawns": "young"},
    "pronghorn": {"bucks": "adult_male", "does": "adult_female", "fawns": "young"},
}


def _processed_parquet_exists(
    bucket_name: str,
    state: str,
    species: str,
    year: int,
    season: str,
    filename_base: str,
) -> bool:
    """
    Checks if the processed Parquet file already exists in S3.
    """
    key = f"processed/{state}/{species}/harvest/{season}/{year}/{filename_base}.parquet"
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"✅ Processed file already exists: {key}. Skipping reprocessing.")
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            print(f"❌ Error checking processed file: {e}")
            # Optional: TODO SQS DLQ for this error case
            return False


def _process_single_harvest_file(
    bucket_name: str, object_key: str, state: str, species: str, season: str, year: int
) -> None:
    print(f"🚀 Ingesting {species} harvest data for {state.upper()} {year}")

    job_id = start_textract_pdf_analysis(bucket_name, object_key)
    status = wait_for_job(job_id)

    if status != "SUCCEEDED":
        print(f"❌ Textract job {job_id} failed for {object_key}. Skipping.")
        # TODO: Add to DLQ for reprocessing later
        sys.exit(1)

    blocks = get_blocks(job_id)
    full_table = extract_table_rows(blocks)
    df = rows_to_data_frame(full_table, state, species, year, season)
    write_parquet_to_s3(df, bucket_name, object_key, state, species, year, season)

    print(f"✅ Successfully processed: {object_key}")


def _get_raw_harvest_pdfs(
    bucket_name: str, state: str, species: str, season: str
) -> List[str]:
    """
    Returns all raw harvest PDF file names in S3 for a specific state, species and season.
    """
    prefix = f"raw/{state}/{species}/harvest/{season}"
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    pdf_files = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".pdf"):
                pdf_files.append(key)

    return pdf_files


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket_name: str,
    object_key: str,
    state: str,
    species: str,
    year: int,
    season: str,
) -> None:
    """
    Writes the DataFrame as Parquet to S3 under the structured processed data path.
    """
    # Construct destination path
    source_basename = os.path.splitext(os.path.basename(object_key))[0]
    s3_path = f"s3://{bucket_name}/processed/{state}/{species}/harvest/{season}/{year}/{source_basename}.parquet"

    print(f"Writing parquet to: {s3_path}")

    # Write Parquet to S3
    df.to_parquet(s3_path, index=False, storage_options={"profile": "default"})

    print(f"Successfully wrote parquet to {s3_path}")


def start_textract_pdf_analysis(bucket_name: str, object_key: str) -> str:
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket_name, "Name": object_key}},
        FeatureTypes=["TABLES"],  # only tables, not forms
    )

    job_id = response["JobId"]

    print(f"Textract job id {job_id}")

    return job_id


def wait_for_job(job_id: str) -> str:
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        print(f"Job status: {status}")

        if status in ["SUCCEEDED", "FAILED"]:
            return status
        time.sleep(10)


def get_blocks(job_id: str) -> List[Dict[str, Any]]:
    all_blocks = []
    next_token = None

    while True:
        if next_token:
            response = textract.get_document_analysis(
                JobId=job_id, NextToken=next_token
            )
        else:
            response = textract.get_document_analysis(JobId=job_id)

        all_blocks.extend(response["Blocks"])
        next_token = response.get("NextToken")

        if not next_token:
            break

    return all_blocks


def extract_table_rows(blocks: List[Dict[str, Any]]) -> List[List[str]]:
    # Group blocks by page number
    pages = defaultdict(list)
    for b in blocks:
        page_num = b.get("Page", 1)
        pages[page_num].append(b)

    # Prep table structure
    table_grid = defaultdict(dict)
    row_offset = 0

    for page_num in sorted(pages.keys()):
        page_blocks = pages[page_num]
        word_map = {b["Id"]: b["Text"] for b in blocks if b["BlockType"] == "WORD"}
        cells = [b for b in page_blocks if b["BlockType"] == "CELL"]

        max_row = max((cell["RowIndex"] for cell in cells), default=0)

        for cell in cells:
            adjusted_row = cell["RowIndex"] + row_offset
            col = cell["ColumnIndex"]

            # Get CHILD word IDs from the cell's relationships
            child_ids = []
            for rel in cell.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    child_ids.extend(rel["Ids"])

            # Get the text content from the word_map
            cell_data = " ".join([word_map.get(cid, "") for cid in child_ids])
            table_grid[adjusted_row][col] = cell_data

        row_offset += max_row

    # Convert to a list of lists (table format)
    max_col = max(col for row in table_grid.values() for col in row)
    table = []

    for row_index in sorted(table_grid.keys()):
        row = [
            table_grid[row_index].get(col_index, "")
            for col_index in range(1, max_col + 1)
        ]
        table.append(row)

    return table


def _clean_headers(headers: List[str]) -> List[str]:
    cleaned = []
    for h in headers:
        h_clean = h.strip().lower().replace(" ", "_")
        h_clean = re.sub(r"[^a-z0-9_]", "", h_clean)
        cleaned.append(h_clean)
    return cleaned


def rows_to_data_frame(
    table: List[List[str]], state: str, species: str, year: int, season: str
) -> pd.DataFrame:
    """
    Converts Textract table output into a normalized DataFrame ready for harvest ingestion.
    """

    raw_headers = table[0]
    headers = _clean_headers(raw_headers)

    data_rows = table[1:]

    filtered_rows = [row for row in data_rows if row[0].strip().lower() != "total"]

    df = pd.DataFrame(filtered_rows, columns=headers)

    # Add metadata
    df["state"] = state
    df["species"] = species
    df["year"] = year
    df["season"] = season

    if "unit" in df.columns:
        df["unit"] = (
            df["unit"]
            .apply(lambda x: int(x.lstrip("0")) if x.strip().isdigit() else pd.NA)
            .astype("Int64")
        )

        df = df[df["unit"].notnull()]
    else:
        print("⚠️ Warning: 'unit' column missing; downstream ingestion may fail.")

    species_sex_map = SPECIES_HARVEST_COLUMN_MAP.get(species.lower())

    if species_sex_map is None:
        raise ValueError(f"Unsupported species '{species}' for harvest ingestion.")

    for src_col, target_col in species_sex_map.items():
        if src_col in df.columns:
            df = df.rename(columns={src_col: target_col})
        else:
            df[target_col] = 0  # if missing, assume 0 for that count.

    numeric_cols = [
        "adult_male",
        "adult_female",
        "young",
        "total_harvest",
        "total_hunters",
        "percent_success",
        "total_rec_days",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].str.replace(",", ""), errors="coerce"
            ).astype("Int64")

    return df


if __name__ == "__main__":
    bucket_name = os.environ.get("HERD_KNOWLEDGE_BUCKET")

    if not bucket_name:
        raise EnvironmentError("HERD_KNOWLEDGE_BUCKET env var not set.")

    parser = argparse.ArgumentParser(
        description="Ingest harvest data via Textract and store as Parquet in S3."
    )
    parser.add_argument("--state", required=True, help="State abbreviation, e.g., 'co'")
    parser.add_argument(
        "--species", required=True, help="Species, e.g., 'deer', 'elk', 'pronghorn'"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=False,
        help="Year of data, e.g., 2024. If omitted, batch processes all available raw files for state/species.",
    )
    parser.add_argument(
        "--season", required=True, help="Season, e.g., 'archery', 'rifle'"
    )

    args = parser.parse_args()

    state = args.state.lower()
    species = args.species.lower()
    year = args.year
    season = args.season.lower()

    full_state = STATE_MAP.get(state.lower())

    if not full_state:
        raise ValueError(f"State '{state}' not supported yet.")

    if year:

        raw_file = f"{full_state}_{season}_{species}_harvest_{year}.pdf"
        object_key = f"raw/{state}/{species}/harvest/{season}/{raw_file}"
        filename_base = os.path.splitext(os.path.basename(raw_file))[0]

        if _processed_parquet_exists(
            bucket_name, state, species, year, season, filename_base
        ):
            print(
                "✅ Already processed: colorado_archery_elk_harvest_2024. Skipping..."
            )
            sys.exit(0)
        else:
            _process_single_harvest_file(
                bucket_name, object_key, state, species, season, year
            )
    else:
        # Batch Processing: process all raw files not yet processed.
        raw_files = _get_raw_harvest_pdfs(bucket_name, state, species, season)

        for raw_file in raw_files:
            filename_base = os.path.splitext(os.path.basename(raw_file))[0]
            year_match = re.search(r"(\d{4})", raw_file)
            if year_match:
                year_extracted = year_match.group(1)
                if _processed_parquet_exists(
                    bucket_name,
                    state,
                    species,
                    int(year_extracted),
                    season,
                    filename_base,
                ):
                    print(f"✅ Already processed: {raw_file}. Skipping...")
                    continue
                else:
                    _process_single_harvest_file(
                        bucket_name,
                        raw_file,
                        state,
                        species,
                        season,
                        int(year_extracted),
                    )
            else:
                print(f"❌ Could not extract year from filename: {raw_file}")
                # TODO: Send this to DLQ for inspection later
