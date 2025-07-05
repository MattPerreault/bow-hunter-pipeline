import boto3
import os
import pandas as pd
import re
import time

from collections import defaultdict

# TODO: Trigger on S3 put.
s3_bucket = "grand-lake"
s3_key = "raw/elk/co/harvest/archery/colorado_archery_elk_harvest_2024.pdf"


session = boto3.Session(profile_name="default")
textract = session.client("textract")


def start_textract_pdf_analysis():
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": s3_bucket, "Name": s3_key}},
        FeatureTypes=["TABLES"],  # only tables, not forms
    )

    job_id = response["JobId"]

    print(f"Textract job id {job_id}")

    return job_id


def wait_for_job(job_id):
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        print(f"Job status: {status}")

        if status in ["SUCCEEDED", "FAILED"]:
            return status
        time.sleep(10)


def get_blocks(job_id):
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


def extract_table_rows(blocks):
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


def _clean_headers(headers):
    cleaned = []
    for h in headers:
        h_clean = h.strip().lower().replace(" ", "_")
        h_clean = re.sub(r"[^a-z0-9_]", "", h_clean)
        cleaned.append(h_clean)
    return cleaned


def rows_to_data_frame(table):
    raw_headers = table[0]
    headers = _clean_headers(raw_headers)

    data_rows = table[1:]
    filtered_rows = [row for row in data_rows if row[0].strip().lower() != "total"]

    df = pd.DataFrame(filtered_rows, columns=headers)

    df["year"] = 2024
    df["season"] = "archery"

    df["unit"] = (
        df["unit"]
        .apply(lambda x: int(x.lstrip("0")) if x.strip().isdigit() else pd.NA)
        .astype("Int64")
    )

    df = df[df["unit"].notnull()]

    numeric_cols = [
        "bulls",
        "cows",
        "calves",
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
    job_id = start_textract_pdf_analysis()
    wait_for_job(job_id)

    blocks = get_blocks(job_id)

    full_table = extract_table_rows(blocks)

    df = rows_to_data_frame(full_table)

    # TODO: Put to S3.
    # Create and write processed data to parquet file
    parquet_dir = f"./data/processed/elk/harvest/archery/2024"
    os.makedirs(parquet_dir, exist_ok=True)
    parquet_file_path = os.path.join(
        parquet_dir, f"colorado_archery_elk_harvest_2024.parquet"
    )

    df.to_parquet(parquet_file_path, index=False)
