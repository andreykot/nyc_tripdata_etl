create temp table split_data as
select
  generate_uuid() as id,
  t.*
from `carto-task.nyc_tlc_akot.tripdata` t
where t.uploaded = {}
;

insert into `carto-task.nyc_tlc_akot.tripdata_geom`
select
  id,
  ST_GEOGPOINT(pickup_longitude, pickup_latitude) as pickup_point,
  ST_GEOGPOINT(dropoff_longitude, dropoff_latitude) as dropoff_point,
  uploaded
from split_data
;

insert into `carto-task.nyc_tlc_akot.tripdata_info`
select
  id,
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RateCodeID,
  store_and_fwd_flag,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  uploaded
from split_data
;