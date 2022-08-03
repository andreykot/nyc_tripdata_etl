import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, BooleanType, StringType
from pyspark.sql import functions as f
from pyspark.sql.functions import when


logging.basicConfig(level=logging.INFO)


DEFAULT_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), nullable=False),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=False),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=False),
    StructField("passenger_count", IntegerType(), nullable=False),
    StructField("trip_distance", FloatType(), nullable=False),
    StructField("pickup_longitude", FloatType(), nullable=False),
    StructField("pickup_latitude", FloatType(), nullable=False),
    StructField("RateCodeID", IntegerType(), nullable=False),
    StructField("store_and_fwd_flag", StringType(), nullable=False),
    StructField("dropoff_longitude", FloatType(), nullable=False),
    StructField("dropoff_latitude", FloatType(), nullable=False),
    StructField("payment_type", IntegerType(), nullable=False),
    StructField("fare_amount", FloatType(), nullable=False),
    StructField("extra", FloatType(), nullable=False),
    StructField("mta_tax", FloatType(), nullable=False),
    StructField("tip_amount", FloatType(), nullable=False),
    StructField("tolls_amount", FloatType(), nullable=False),
    StructField("improvement_surcharge", FloatType(), nullable=False),
    StructField("total_amount", FloatType(), nullable=False),
])


def iter_csv_tripdata(files) -> (str, bool):
    for path in files:
        file_name, file_ext = os.path.splitext(os.path.basename(path))
        is_head_file = file_name.endswith("_00")
        if file_ext.lower() == '.csv':
            yield path, is_head_file
        else:
            logging.info(f"{path}: invalid file type, skipped.")
            continue


def filter_invalid_timestamps(df: DataFrame) -> DataFrame:
    ts_cols = [item[0] for item in df.dtypes if item[1] == 'timestamp']
    for ts_col in ts_cols:
        df = df.filter(f.col(ts_col) > '1900-01-01')
    return df


def filter_invalid_categories(df: DataFrame) -> DataFrame:
    vendor_ids = [1, 2]
    rate_codes = [1, 2, 3, 4, 5, 6]
    payments_types = [1, 2, 3, 4, 5, 6]
    df = (df
          .filter(f.col("VendorID").isin(vendor_ids))
          .filter(f.col("RateCodeID").isin(rate_codes))
          .filter(f.col("payment_type").isin(payments_types))
          )
    return df


def replace_store_and_fwd_flag_by_bool(df: DataFrame) -> DataFrame:
    df = (df
          .withColumn('store_and_fwd_flag',
                      when(f.col('store_and_fwd_flag') == 'Y', True)
                      .when(f.col('store_and_fwd_flag') == 'N', False)
                      .otherwise(None)
                      .cast(BooleanType())
                      )
          )
    return df


def replace_invalid_location_values(df: DataFrame) -> DataFrame:
    location_cols = [item[0] for item in df.dtypes if item[1].endswith('longitude') or item[1].endswith('latitude')]
    for c in location_cols:
        df = (df
              .withColumn(c,
                          when(f.col(c) == 0, None)
                          .when(f.col(c) < -90, None)
                          .when(f.col(c) > 90, None)
                          .otherwise(f.col(c))))
    return df


def clean(df: DataFrame) -> DataFrame:
    df = filter_invalid_timestamps(df)
    df = filter_invalid_categories(df)
    df = replace_store_and_fwd_flag_by_bool(df)
    df = replace_invalid_location_values(df)
    return df


def main(csv_file, is_head_file, output_dir, timestamp: datetime):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Folder {output_dir} was created")

    spark = SparkSession.builder.getOrCreate()
    df = (spark
          .read
          .options(header=is_head_file, delimiter='\t', mode="DROPMALFORMED")
          .schema(schema=DEFAULT_SCHEMA)
          .csv(csv_file))
    df = clean(df)
    df = df.withColumn("uploaded", f.lit(timestamp))

    name, ext = os.path.splitext(os.path.basename(csv_file))
    parquet_file = os.path.join(output_dir, name + '.parquet')
    df.coalesce(1).write.mode("overwrite").parquet(parquet_file)
    logging.info(f"{csv_file}: {parquet_file} was created.")


if __name__ == "__main__":
    pass
