with 
	mindate(date) as (select MIN(w.date) from public.climate w),
    maxdate(date) as (select MAX(w.date) from public.climate w),
    dates(date) as (select * from generate_series((select date from mindate), (select date from maxdate), '1 day'))
INSERT INTO weather_schema.Dates(DateId, FullDate, Year, Quarter, Month, Day)
select
	date_part('year', date) * 10000 + date_part('month', date) * 100 + date_part('day', date),
	date,
	date_part('year', date),
	cast(date_part('month', date) / 3 as int) + 1,
	date_part('month', date),
	date_part('day', date)
FROM
	dates;