-- climate
insert into weather_schema.climate(dateid, stationid, provinceid, mintemperature, maxtemperature, avgtemperature, avghumidity, rainfall, sunshinedirection, maxwindspeed, avgwindspeed, winddirection, mostwinddirection)
select
	d.dateid,
	c.station_id,
	p.province_id,
	c.tn,
	c.tx,
	c.tavg,
	c.rh_avg,
	c.rr,
	c.ss,
	c.ff_x,
	c.ff_avg,
	c.ddd_x,
	c.ddd_car
from 
	public.climate c
	left join weather_schema.dates d on d.fulldate = c.date 
	left join public.station s on s.station_id = c.station_id
	left join public.province p on s.province_id = p.province_id 