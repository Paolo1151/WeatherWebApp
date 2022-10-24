-- province
insert into weather_schema.province
select
	p.province_id,
	p.province_name
from
	public.province p