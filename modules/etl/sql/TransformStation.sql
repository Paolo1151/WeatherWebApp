-- station
insert into weather_schema.station
select
	s.station_id,
	s.station_name,
	s.region_name,
	s.latitude,
	s.longitude
from 
	public.station s