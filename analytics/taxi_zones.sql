select
  z.zone_name,
  avg(ti.tip_amount) as avg_tip_amount
from `carto-task.nyc_tlc_akot.tripdata_geom` tg
join `nyc_tlc_akot.tripdata_info` ti
  on tg.id = ti.id
join `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` z
  on ST_WITHIN(tg.pickup_point, z.zone_geom)
group by z.zone_name
order by avg(ti.tip_amount) desc
limit 10

/*
Results:

zone_name                           avg_tip_amount
Newark Airport                      9.8868271634278582
Charleston/Tottenville              8.3100002288818366
Rossville/Woodrow                   7.5480001449584968
Eltingville/Annadale/Prince's Bay	6.9779998779296877
Pelham Bay Park	                    6.81170726349441
Sunset Park East	                6.7967613618785627
Country Club	                    6.7657894586261955
West Village	                    6.5505337225845111
Dyker Heights	                    6.4801498114546474
Baisley Park	                    5.7958702477694617
*/