select
  avg(fare_amount / trip_distance) as avg_fare_per_mile
from `carto-task.nyc_tlc_akot.tripdata_info`
where trip_distance <> 0

/*
Results:
avg_fare_per_mile
6.3637794946195205
*/