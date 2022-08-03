create or replace table carto-task.nyc_tlc_akot.tripdata (
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance FLOAT64,
  pickup_longitude FLOAT64,
  pickup_latitude FLOAT64,
  RateCodeID INTEGER,
  store_and_fwd_flag BOOLEAN,
  dropoff_longitude FLOAT64,
  dropoff_latitude FLOAT64,
  payment_type INTEGER,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  uploaded TIMESTAMP
);

create or replace table carto-task.nyc_tlc_akot.tripdata_geom(
  id STRING,
  pickup_point GEOGRAPHY
  dropoff_point GEOGRAPHY
  uploaded TIMESTAMP
);

create or replace table carto-task.nyc_tlc_akot.tripdata_info (
  id STRING,
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance FLOAT64,
  RateCodeID INTEGER,
  store_and_fwd_flag BOOLEAN,
  payment_type INTEGER,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  uploaded TIMESTAMP
);