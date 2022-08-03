import configparser
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from google.cloud import bigquery, storage
from google.oauth2 import service_account


logging.basicConfig(level=logging.INFO)


def get_gc_credentials(service_key_path):
    return service_account.Credentials.from_service_account_file(
        service_key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def upload_blob(storage_client, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logging.info(
        f"{bucket_name}: File {source_file_name} uploaded to {destination_blob_name}."
    )


def upload_files_to_cloud_storage(
        credentials_file: str,
        bucket_name: str,
        prefix: str,
        files_dir: str,
        ext: str = ".parquet",
        max_concurrent=5
) -> list:
    credentials = get_gc_credentials(credentials_file)
    client = storage.Client(credentials=credentials, project=credentials.project_id)

    uploaded = list()
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        for root, dirs, files in os.walk(files_dir):
            for file in filter(lambda x: os.path.splitext(x)[1] == ext, files):
                blob = os.path.join(prefix, os.path.basename(root))
                executor.submit(upload_blob, client, bucket_name, os.path.join(root, file), blob)
                uploaded.append(os.path.join('gs://', bucket_name, blob))
    logging.info(f"Cloud Storage {bucket_name}: uploaded {len(uploaded)} files successfully.")
    return uploaded


def upload_files_to_bigquery(
        credentials_file: str,
        table_name: str,
        cloud_files: list
):
    credentials = get_gc_credentials(credentials_file)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )

    for uri in cloud_files:
        load_job = client.load_table_from_uri(
            uri, table_name, job_config=job_config
        )
        load_job.result()
        logging.info(f"{table_name}: object {uri} loaded to the {table_name} table")
    logging.info(f"BigQuery {table_name}: all objects uploaded successfully.")


def split_nyc_tripdata(
        credentials_file: str,
        sql_file: str,
        uploaded_timestamp: datetime
):
    credentials = get_gc_credentials(credentials_file)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    with open(sql_file, 'r') as f:
        sql = f.read().format(f"'{uploaded_timestamp}'")

    query_job = client.query(sql)
    query_job.result()


if __name__ == '__main__':
    pass
