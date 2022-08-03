### nyc_tripdata_etl

ETL pipeline for NYC trip data prepared for Apache Airflow (version 2.3.3).

- main DAG: `dags/etl_dag.py`
- the DAG description: `dags/README.md`
- Python scripts used in the DAG: `dags/scripts`
- SQL code used in the DAG: `dags/sql`

Data quality issues:

- Invalid value types in columns for specific data types (floats in RateCodeID, payment_type). Such rows skipped during the transformation process with mode "DROPMALFORMED" in Spark.
- Invalid category values and nulls (RateCodeID, payment_type). Skipped.
- Invalid coordinates (zeros, less than -90, greater than 90). Replaced by NULLs.
- Default timestamps like '1900-01-01', Replaced by NULLs.

Example of research in `data_quality/data_quality_check_example.ipynb`.

Other data issues can be found 

SQL queries to answer following questions:

- What is the average fare per mile?: 
`analytics/fare_per_mile.sql`
- Which are the 10 pickup taxi zones with the highest average tip?: 
`analytics/taxi_zones.sql`

Before executing this DAG you need to set up and initialize custom Airflow Variables from `airflow_variables.env`.
