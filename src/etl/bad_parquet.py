import duckdb
import boto3
import os


def find_stale_parquet_files(bucket_name):
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    prefix = "processed/"
    stale_files = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "/population/" in key:
                s3_path = f"s3://{bucket_name}/{key}"
                print(f"Checking: {s3_path}")

                try:
                    query = f"""
                        SELECT * FROM read_parquet('{s3_path}') LIMIT 1
                    """
                    df = duckdb.query(query).to_df()
                    if "male_female_ratio" not in df.columns:
                        print(f"❌ Missing 'male_female_ratio': {s3_path}")
                        stale_files.append(s3_path)
                except Exception as e:
                    print(f"Error reading {s3_path}: {e}")

    if stale_files:
        print("\n🚩 Stale parquet files found (missing 'male_female_ratio'):")
        for f in stale_files:
            print(f)
    else:
        print("\n✅ All parquet files contain 'male_female_ratio'.")


if __name__ == "__main__":
    bucket_name = os.environ.get("HERD_KNOWLEDGE_BUCKET")
    if not bucket_name:
        raise EnvironmentError("HERD_KNOWLEDGE_BUCKET env var not set.")

    find_stale_parquet_files(bucket_name)
