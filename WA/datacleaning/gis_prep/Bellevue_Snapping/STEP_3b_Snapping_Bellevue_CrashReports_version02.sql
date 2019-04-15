


-- =============================================================================================================
-- STEP 1: Get distinct list of Crash reports which snap to a road segment with the same name of the lowest rank. 
-- =============================================================================================================

-- 1,912 
SELECT distinct a.*
--into #temp 
FROM [temp].[dbo].[Crashes_NearTable] a
join [temp].[dbo].[Crashes_NearTable] b on a.INDEXED_PRIMARY_TRAFFICWAY = b.OfficialSt and a.objectid = b.objectid 


select * from #temp 


-- 1,226 distinct Crash reports 
select a.*
--into #temp2
from #temp a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst 
	from #temp 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


-- list of crashes which snap to a road segment with the same name and the rank = 1 (e.g. closest segment) 
select * from #temp2 where near_rank = '1' order  by near_rank  -- 933 



-- =============================================================================================================
-- STEP 2: Get distinct list of Crash reports which do not snap to a road segment with the same name. 
-- =============================================================================================================

SELECT distinct a.*
--into #temp3
FROM [temp].[dbo].[Crashes_NearTable] a
join [temp].[dbo].[Crashes_NearTable] b on a.INDEXED_PRIMARY_TRAFFICWAY <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp2 order by in_fid  
select * from #temp3 order by in_fid  


--154 distinct crashes do not match to a segment with the same street name 
select DISTINCT a.IN_FID  
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) order by a.in_fid 



-- =============================================================================================================
-- STEP 3: Get a list of unmatched road names of snapped Crash reports on the Bellevue road network. 
-- =============================================================================================================

-- find unmatch streets (65 pairs) 
select a.INDEXED_PRIMARY_TRAFFICWAY, a.officialst, COUNT(*) as COUNT 
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 
GROUP BY a.INDEXED_PRIMARY_TRAFFICWAY, a.OFFICIALST 
order by COUNT(*) DESC  



-- IN ARCGIS: 
--------------
-- Review the unmatched streets to determine if they are the same streets, 
-- but different names, or if they are in fact, different streets.  
-- If they are the same streets, change the street name in the street_updated column (see below).  


-- create temp table from #temp3 --> 65 pairs (314 total rows)
select a.* 
--into #temp4
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 


-- add a new field called 'street_updated' and populate it with the values from the Waze street column. 
alter table #temp4 add INDEXED_PRIMARY_TRAFFICWAY_updated varchar(50) 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = INDEXED_PRIMARY_TRAFFICWAY


select * from #temp4 



-- find unmatch streets (65 pairs) 
select a.INDEXED_PRIMARY_TRAFFICWAY, a.officialst, COUNT(*) as COUNT 
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 
GROUP BY a.INDEXED_PRIMARY_TRAFFICWAY, a.OFFICIALST 
order by COUNT(*) DESC  



update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NE BELLEVUE-REDMOND RD' where INDEXED_PRIMARY_TRAFFICWAY = 'BEL RED RD' and OfficialSt = 'NE BELLEVUE-REDMOND RD' -- 102 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE HILLS CN' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE HILLS CONNECTOR' and OfficialSt = 'LAKE HILLS CN' -- 37 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'COAL CREEK PKWY SE' where INDEXED_PRIMARY_TRAFFICWAY = 'SE COAL CREEK PKWY' and OfficialSt = 'COAL CREEK PKWY SE' -- 14 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD SE' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE WASHINGTON BLVD' and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 10 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'W LAKE SAMMAMISH PKWY SE' where INDEXED_PRIMARY_TRAFFICWAY = 'W LAKE SAMMAMISH PKW' and OfficialSt = 'W LAKE SAMMAMISH PKWY SE' -- 9 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE COAL CREEK PKWY' where INDEXED_PRIMARY_TRAFFICWAY = 'COAL CREEK PKWY SE' and OfficialSt = 'SE COAL CREEK PKWY' -- 4 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'W LAKE SAMMAMISH PKWY NE' where INDEXED_PRIMARY_TRAFFICWAY = 'W LAKE SAMMAMISH PKW' and OfficialSt = 'W LAKE SAMMAMISH PKWY NE' -- 4 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'RICHARDS RD' where INDEXED_PRIMARY_TRAFFICWAY = '132ND AVE SE' and OfficialSt = 'LAKE WASHINGTON BLVD NE' -- 3 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD NE' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE WASHINGTON BLVD' and OfficialSt = 'RICHARDS RD' -- 3 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE COUGAR MOUNTAIN WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'SE COUGAR MOUNTAIN W' and OfficialSt = 'SE COUGAR MOUNTAIN WAY' -- 2 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE WOLVERINE WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'WOLVERINE WAY' and OfficialSt = 'SE WOLVERINE WAY' -- 2 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '130TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = '130TH PL SE AT SE 7T' and OfficialSt = '130TH PL SE' -- 2 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NE 40TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'NE 40TH ST AT 132ND' and OfficialSt = 'NE 40TH ST' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '116TH AVE NE' where INDEXED_PRIMARY_TRAFFICWAY = '100 116TH AVE NE' and OfficialSt = '116TH AVE NE' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NORTHUP WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'NORTHUP WAY/NE 20TH' and OfficialSt = 'NORTHUP WAY' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'RICHARDS RD' where INDEXED_PRIMARY_TRAFFICWAY = 'RICHARDS RD SE' and OfficialSt = 'RICHARDS RD' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE 36TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'SE 38TH ST' and OfficialSt = 'SE 36TH ST' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '127TH AVE SE' where INDEXED_PRIMARY_TRAFFICWAY = 'SE 62ND ST' and OfficialSt = '127TH AVE SE' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD SE' where INDEXED_PRIMARY_TRAFFICWAY = '118TH AVE SE' and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '136TH AVE SE' where INDEXED_PRIMARY_TRAFFICWAY = '136TH AVE NE' and OfficialSt = '136TH AVE SE' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '139TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = 'KAMBER RD' and OfficialSt = '139TH PL SE' -- 1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '120TH AVE NE' where INDEXED_PRIMARY_TRAFFICWAY = 'NE 1ST ST' and OfficialSt = '120TH AVE NE' -- 1 crash




SELECT distinct a.*, b.INDEXED_PRIMARY_TRAFFICWAY_updated
into #temp5
FROM [temp].[dbo].[Crashes_NearTable] a
full join #temp4 b on a.objectid = b.objectid 


update #temp5 set INDEXED_PRIMARY_TRAFFICWAY_updated = INDEXED_PRIMARY_TRAFFICWAY where INDEXED_PRIMARY_TRAFFICWAY_updated is null  -- 3,125


select * from #temp5 order by OBJECTID 




-- =============================================================================================================
-- STEP 4: Re-run Step 1 but using updated street name for matching. 
-- =============================================================================================================


-- get distinct list of Bellevue Crash reports which snap to a road name using the lowest near_rank value. 
SELECT distinct a.*
--into #temp6 
FROM #temp5 a
join #temp5 b on UPPER(a.INDEXED_PRIMARY_TRAFFICWAY_updated) = b.OfficialSt and a.objectid = b.objectid 




-- 1,369 distinct Waze reports 
select a.*
--into #temp7
from #temp6 a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp6 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst, INDEXED_PRIMARY_TRAFFICWAY_updated 
	from #temp6 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst, INDEXED_PRIMARY_TRAFFICWAY_updated 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 



SELECT distinct a.*
into #temp8
FROM #temp5 a
join #temp5 b on a.INDEXED_PRIMARY_TRAFFICWAY <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp2 order by in_fid  
select * from #temp3 order by in_fid  


--155 distinct crashes do not match to a segment with the same street name 
select DISTINCT a.IN_FID
from #temp8 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp7 b 
	where a.in_fid = b.in_fid 
) order by a.in_fid 



--NOT FIXED
--------------
--1026, 1027, 271, 355, 674, 757, 801, 809


--FIXED
---------
--1032, 117, 1218, 1238, 1285, 1304, 1341, 1342, 1376, 1377, 1378, 1379, 1387, 147, 205, 209, 210, 214, 720, 723, 748, 810, 843, 844



-- find unmatch streets (66 pairs) 
select a.INDEXED_PRIMARY_TRAFFICWAY, a.officialst, COUNT(*) as COUNT 
from #temp8 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp7 b 
	where a.in_fid = b.in_fid 
) 
GROUP BY a.INDEXED_PRIMARY_TRAFFICWAY, a.OFFICIALST 
order by COUNT(*) DESC  