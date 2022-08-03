from datetime import datetime

from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.python import get_current_context

from scripts import ingest, transform, upload


@dag(
    dag_id="etl",
    schedule_interval="@once",
    start_date=datetime(2022, 2, 2),
    max_active_runs=1,
    catchup=False
)
def etl():

    @task()
    def download_data():
        downloaded_files = ingest.download_files_from_public_gs_bucket(
            bucket_name=Variable.get("SOURCE_BUCKET"),
            destination_folder=Variable.get("INPUT_DIR"),
            max_concurrent=5,
        )
        return downloaded_files

    @task()
    def get_downloaded_files(files: list):
        from scripts.transform import iter_csv_tripdata

        return [(path, is_head_file)
                for path, is_head_file in iter_csv_tripdata(files)]

    @task()
    def transform_data(item, output_dir):
        context = get_current_context()
        transform.main(
            csv_file=item[0],
            is_head_file=item[1],
            output_dir=output_dir,
            timestamp=context['execution_date']
        )
        return True

    @task()
    def upload_files_to_cloud_storage(flag: bool) -> list[str]:
        context = get_current_context()
        uploaded_files = upload.upload_files_to_cloud_storage(
            credentials_file=Variable.get("GOOGLE_SERVICE_ACCOUNT_KEY"),
            bucket_name=Variable.get("LANDING_ZONE_BUCKET"),
            prefix=context['execution_date'].strftime("%Y-%m-%d"),
            files_dir=Variable.get("OUTPUT_DIR")
        )
        return uploaded_files

    @task()
    def upload_files_to_bigquery(files: list[str]):
        upload.upload_files_to_bigquery(
            credentials_file=Variable.get("GOOGLE_SERVICE_ACCOUNT_KEY"),
            table_name=Variable.get("BIGQUERY_NYC_TLC_TABLE"),
            cloud_files=files
        )

    @task()
    def split_nyc_tripdata():
        context = get_current_context()
        upload.split_nyc_tripdata(
            credentials_file=Variable.get("GOOGLE_SERVICE_ACCOUNT_KEY"),
            sql_file="./sql/split.sql",
            uploaded_timestamp=context['execution_date']
        )

    files = download_data()

    is_transform = transform_data \
        .partial(output_dir=Variable.get("OUTPUT_DIR")) \
        .expand(item=get_downloaded_files(files))

    upload_files_to_bigquery(upload_files_to_cloud_storage(is_transform)) >> split_nyc_tripdata()


dag = etl()
