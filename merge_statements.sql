--merge to ods - weather forecast
insert into ods_weather_forecast (station_name,station_id,weather_tstamp,tmp,tmax_degc,tmin_degc,rain_mm_3h)
with fc_base as (
		select
			weather_data->'city'->>'name' station_name
			,weather_data->'city'->>'id' station_id
			,weather_data->'list' list
		from stg_fc
	),
	fc_flatten_json as (
	select
		station_name
		,station_id
		,json_array_elements(list) fc_data
	from fc_base
	)
	select
		station_name
		,cast(station_id as int) station_id
		,to_timestamp(cast(fc_data->>'dt' as int)) weather_tstamp
		,cast(fc_data->'main'->>'temp' as double precision) tmp
		,cast(fc_data->'main'->>'temp_max' as double precision) tmax_degc
		,cast(fc_data->'main'->>'temp_min' as double precision) tmin_degc
		,cast(fc_data->'rain'->>'3h' as double precision) rain_mm_3h
	from fc_flatten_json
on conflict (station_id,weather_tstamp)
do update
	set tmp = excluded.tmp
	,tmax_degc = excluded.tmax_degc
	,tmin_degc = excluded.tmin_degc
	,rain_mm_3h = excluded.rain_mm_3h
	,dwh_last_modified = current_timestamp;

--merge to ods - current weather
insert into ods_current_weather (station_name,station_id,weather_tstamp,tmp,tmax_degc,tmin_degc,rain_mm_1h,rain_mm_3h)
select weather_data->>'name' station_name
		,cast(weather_data->>'id' as int) station_id
		,to_timestamp(cast(weather_data->>'dt' as int)) weather_tstamp
		,cast(weather_data->'main'->>'temp' as double precision) tmp
		,cast(weather_data->'main'->>'temp_max' as double precision) tmax_degc
		,cast(weather_data->'main'->>'temp_min' as double precision) tmin_degc
		,cast(weather_data->'rain'->>'1h' as double precision) rain_mm_1h
		,cast(weather_data->'rain'->>'3h' as double precision) rain_mm_3h
from stg_current
on conflict (station_id,weather_tstamp)
do update
	set tmp = excluded.tmp
	,tmax_degc = excluded.tmax_degc
	,tmin_degc = excluded.tmin_degc
	,rain_mm_1h = excluded.rain_mm_1h
	,rain_mm_3h = excluded.rain_mm_3h
	,dwh_last_modified = current_timestamp;

--merge d_weather station
insert into d_weather_station (station_id,station_name)
select distinct w.station_id,w.station_name
from ods_current_weather w
left join d_weather_station ws on w.station_id = ws.station_id
where ws.sk_weather_station is null;


--merge to f_weather_history_monthly
insert into f_weather_history_monthly (sk_date,sk_weather_station,total_rain_mm,min_temp,max_temp)
select cast(to_char(cw.weather_tstamp,'yyyymm') as int) sk_date
		,ws.sk_weather_station
		,sum(rain_mm_3h) total_rain_mm
		,avg(cw.tmin_degc) min_temp
		,max(cw.tmax_degc) max_temp
from ods_current_weather cw
join d_weather_station ws on cw.station_id = ws.station_id
group by 1,2
union all
select cast(to_char(hm.fc_date,'yyyymm') as int) sk_date
		,ws.sk_weather_station
		,hm.rain_mm total_rain_mm
		,hm.tmax_degc
		,hm.tmin_degc
from ods_weather_history_monthly hm
join d_weather_station ws on hm.station_name = ws.station_name
on conflict (sk_date,sk_weather_station)
do update
	set total_rain_mm = excluded.total_rain_mm
	,min_temp = excluded.min_temp
	,max_temp = excluded.max_temp
	,dwh_last_modified = current_timestamp;

--delete historical weather forcast
delete from f_weather_forecast where weather_tstamp < current_timestamp;
--merge new weather forecast
insert into f_weather_forecast (sk_date,weather_tstamp,sk_weather_station,rain_mm_3h,min_temp,max_temp)
select cast(to_char(wf.weather_tstamp,'yyyymm') as int) sk_date
		,wf.weather_tstamp
		,ws.sk_weather_station
		,wf.rain_mm_3h
		,wf.tmin_degc min_temp
		,wf.tmax_degc max_temp
from ods_weather_forecast wf
join d_weather_station ws on wf.station_id = ws.station_id
where wf.weather_tstamp > current_timestamp
on conflict (weather_tstamp,sk_weather_station)
do update
	set rain_mm_3h = excluded.rain_mm_3h
	,min_temp = excluded.min_temp
	,max_temp = excluded.max_temp
	,dwh_last_modified = current_timestamp;
--merge weather history
insert into f_weather_history (sk_date,weather_tstamp,sk_weather_station,rain_mm_3h,min_temp,max_temp)
select cast(to_char(cw.weather_tstamp,'yyyymm') as int) sk_date
		,cw.weather_tstamp
		,ws.sk_weather_station
		,cw.rain_mm_3h
		,cw.tmin_degc min_temp
		,cw.tmax_degc max_temp
from ods_current_weather cw
join d_weather_station ws on cw.station_id = ws.station_id
on conflict (weather_tstamp,sk_weather_station)
do update
	set rain_mm_3h = excluded.rain_mm_3h
	,min_temp = excluded.min_temp
	,max_temp = excluded.max_temp
	,dwh_last_modified = current_timestamp;