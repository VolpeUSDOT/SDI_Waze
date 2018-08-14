---- Validation of counts of accidents -- manual process only here, but could be automated and optimized to test across all states, all months.


--- Test time filter. Notes on 2018-08-12. On 2018-08-13, a few more filled in (re-processed DLQ probably), but no new 2018. Also, running slower now.
---- UT: 47,440 accitdent reports, only 2017 in there now.
---- CT: 174,443 accident reports, also only 2017
---- MD: 1,339,627 reports, up to end of August 12 2018
---- MA: 636,956 reports, up to end of August 12 2018
---- VA: 650,736 reports, only 2017 still

SELECT alert_uuid, alert_type, street, state, location_lat, location_lon, start_pub_utc_timestamp, pub_utc_timestamp, road_type  FROM dw_waze.alert 
 WHERE state='VA' 
 AND alert_type='ACCIDENT'
 AND pub_utc_timestamp BETWEEN to_timestamp('2017-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                       AND     to_timestamp('2018-08-12 23:59:59','YYYY-MM-DD HH24:MI:SS')
 ORDER BY pub_utc_timestamp DESC;
 

---- Same, but just count rows of output

SELECT COUNT(alert_uuid)  FROM dw_waze.alert 
 WHERE state='MD' 
 AND pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                       AND     to_timestamp('2018-07-31 23:59:59','YYYY-MM-DD HH24:MI:SS');
 


---- Count of distinct alert_uuid by state, month, and type. UT: 558751

SELECT COUNT(*) FROM (
  SELECT DISTINCT alert_uuid, state, alert_type FROM dw_waze.alert 
   WHERE state='UT' 
   AND pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                         AND     to_timestamp('2018-07-31 23:59:59','YYYY-MM-DD HH24:MI:SS') 
                         ) distinct_table;
 


---- Count alert types by state, for distinct alert uuid's

SELECT state, alert_type, COUNT(*) FROM (
  SELECT DISTINCT alert_uuid, state, alert_type FROM dw_waze.alert 
   WHERE state IN ('UT', 'VA', 'MD', 'CT') 
   AND pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                         AND     to_timestamp('2018-07-31 23:59:59','YYYY-MM-DD HH24:MI:SS') 
                         ) distinct_table 
  GROUP BY state, alert_type;
 
---- Same, but now also split by month and year

---- First, create staging table
CREATE TABLE public.distinct_table3 AS SELECT DISTINCT alert_uuid, state, alert_type, pub_utc_timestamp FROM dw_waze.alert 
   WHERE state IN ('UT', 'VA', 'MD', 'CT') 
   AND pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                         AND     to_timestamp('2018-07-31 23:59:59','YYYY-MM-DD HH24:MI:SS'); 
                         
---- take a look at this table
select * from public.distinct_table3 limit 50;

---- create year and month columns 
ALTER TABLE public.distinct_table3 
  ADD COLUMN year int;
  
UPDATE public.distinct_table3 set year=date_part(year, pub_utc_timestamp);

ALTER TABLE public.distinct_table3 
  ADD COLUMN month int;
  
UPDATE public.distinct_table3 set month=date_part(month, pub_utc_timestamp);

SELECT state, year, month, alert_type, COUNT(*) FROM public.distinct_table3 
  GROUP BY state, year, month, alert_type;

---- without distinct 


---- First, create staging table
CREATE TABLE public.incident_table AS SELECT alert_uuid, state, alert_type, pub_utc_timestamp FROM dw_waze.alert 
   WHERE state IN ('UT', 'VA', 'MD', 'CT') 
   AND pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
                         AND     to_timestamp('2018-07-31 23:59:59','YYYY-MM-DD HH24:MI:SS'); 
                         
---- take a look at this table
select * from public.incident_table limit 50;

---- create year and month columns 
ALTER TABLE public.incident_table 
  ADD COLUMN year int;
  
UPDATE public.incident_table set year=date_part(year, pub_utc_timestamp);

ALTER TABLE public.incident_table 
  ADD COLUMN month int;
  
UPDATE public.incident_table set month=date_part(month, pub_utc_timestamp);

SELECT state, year, month, alert_type, COUNT(*) FROM public.incident_table 
  GROUP BY state, year, month, alert_type;




