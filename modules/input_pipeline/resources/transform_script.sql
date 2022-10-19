SELECT
	entry_id,
	date,
	Tn as min_temp,
	Tx as max_temp,
	Tavg as avg_temp,
	RH_avg as avg_humidity,
	RR as rainfall,
	ss as sunshine_duration,
	ff_x as max_wind_speed,
	ddd_x as wind_dir,
	ff_avg as avg_wind_speed,
	ddd_Car as most_wind_dir,
	s.station_id
FROM
	climate c
	INNER JOIN station s ON c.station_id = s.station_id
	INNER JOIN province p ON s.province_id = p.province_id