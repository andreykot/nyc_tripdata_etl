### ETL DAG

The ETL DAG was developed with TaskFlow API introduced in Airflow version 2. 
This API helps to build DAGs with Python decorators.

Also, this ETL uses the Airflow feature for generating tasks dynamically which was introduced in Airflow version 2.3.

#### Tasks
1. *download_data*

Downloading cloud files (blobs) from public Google Cloud Storage.
Threading is used to speed up the process. 
Zip archives are unzipping to the target folder and are removing after unzipping CSV files.

2. *get_downloaded_files*

The task checks which CSV files have headers and which files don't. Pushed a list of files to XCom.

3. *transform_data*

The task runs Spark session and applies several transformations. 
Result - a folder with cleaned and transformed parquet files.

For each CSV file Airflow create such task dynamically.
The number of tasks depends on the number of downloaded files at the moment.
For this the "partial-expand" logic is used. 
This logic was introduced in Airflow v. 2.3 and it was interesting for me to try this somewhere.
Pros: we are generating tasks dynamically for some number of files, this tasks can be triggered in parallel by Airflow. 
Cons: we are not able to submit these by SparkSubmitOperator.
As result, this approach can be optimal only with a big number of workers which are available for Airflow (for CeleryExecutor),
but extremely slow on local machine. I left this approach in the code, but it is better to submit spark jobs to Spark cluster or 
prepare parquet files not for each csv file.

4. *upload_files_to_cloud_storage*

The task uploads parquet files to the cloud storage. This storage is used as landing zone.

5. *upload_files_to_bigquery*

Uploads parquet files from Cloud Storage to BigQuery.

6. *split_nyc_tripdata*

The task execute SQL code in BigQuery. 
This code takes the fact table (tripdata), split uploaded data into two tables with generating unique uuid id.
Coordinates is transformed to `GEOGRAPHY` type (points).
