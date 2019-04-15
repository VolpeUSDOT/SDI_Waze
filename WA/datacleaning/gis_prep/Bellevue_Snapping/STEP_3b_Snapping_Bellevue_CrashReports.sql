



-- objectID's (IN_FID) of crash reports which do not have any lat/lon values  
-- 28, 216, 470, 558, 1164, 1232, 1294, 1371, 1385, 1870, 1938, 1976, 2002, 2094, 2121


-- =============================================================================================================
-- STEP 1: Get distinct list of Waze reports which snap to a road segment with the same name of the lowest rank. 
-- =============================================================================================================

-- 2,840 
SELECT distinct a.*
--into #temp 
FROM [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable] a
join [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable] b on a.INDEXED_PRIMARY_TRAFFICWAY = b.OfficialSt and a.objectid = b.objectid 


select * from #temp 


-- 1,861 distinct Crash reports 
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
select * from #temp2 where near_rank = '1' order  by near_rank  --1,454 



-- =============================================================================================================
-- STEP 2: Get distinct list of Waze reports which do not snap to a road segment with the same name. 
-- =============================================================================================================

SELECT distinct a.*
--into #temp3
FROM [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable] a
join [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable] b on a.INDEXED_PRIMARY_TRAFFICWAY <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp2 order by in_fid  
select * from #temp3 order by in_fid  


--231 distinct crashes do not match to a segment with the same street name 
select DISTINCT a.IN_FID  
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) order by a.in_fid 



-- =============================================================================================================
-- STEP 3: Get a list of unmatched road names of snapped Waze reports on the Bellevue road network. 
-- =============================================================================================================

-- find unmatch streets (81 pairs) 
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


-- create temp table with the original near table upload. 
select * 
--into #temp4
from [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable]



-- add a new field called 'street_updated' and populate it with the values from the Waze street column. 
alter table #temp4 add INDEXED_PRIMARY_TRAFFICWAY_updated varchar(50) 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = INDEXED_PRIMARY_TRAFFICWAY


select * from #temp4 

-- update the street name if the segments are the same, or if the segments have almost identical names. 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NE BELLEVUE-REDMOND RD' where INDEXED_PRIMARY_TRAFFICWAY = 'BEL RED RD' and OfficialSt = 'NE BELLEVUE-REDMOND RD' -- 151 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE HILLS CN' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE HILLS CONNECTOR' and OfficialSt = 'LAKE HILLS CN' -- 67 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'W LAKE SAMMAMISH PKWY SE' where INDEXED_PRIMARY_TRAFFICWAY = 'W LAKE SAMMAMISH PKW' and OfficialSt = 'W LAKE SAMMAMISH PKWY SE' -- 27 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'COAL CREEK PKWY SE' where INDEXED_PRIMARY_TRAFFICWAY = 'SE COAL CREEK PKWY' and OfficialSt = 'COAL CREEK PKWY SE' -- 14 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD SE' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE WASHINGTON BLVD' and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 11 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE COAL CREEK PKWY' where INDEXED_PRIMARY_TRAFFICWAY = 'COAL CREEK PKWY SE' and OfficialSt = 'SE COAL CREEK PKWY' -- 8 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'W LAKE SAMMAMISH PKWY NE' where INDEXED_PRIMARY_TRAFFICWAY = 'W LAKE SAMMAMISH PKW' and OfficialSt = 'W LAKE SAMMAMISH PKWY NE' -- 6 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'RICHARDS RD' where INDEXED_PRIMARY_TRAFFICWAY = '132ND AVE SE' and OfficialSt = 'RICHARDS RD' --  4 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD NE' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE WASHINGTON BLVD' and OfficialSt = 'LAKE WASHINGTON BLVD NE' --  3 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE EASTGATE WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'SE EASTGATE WAY AKA' and OfficialSt = 'SE EASTGATE WAY' --  2 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '130TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = '130TH PL SE AT SE 7T' and OfficialSt = '130TH PL SE' --  2 crashes 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '140TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = 'KAMBER RD' and OfficialSt = '140TH PL SE' --  2 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE COUGAR MOUNTAIN WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'SE COUGAR MOUNTAIN W' and OfficialSt = 'SE COUGAR MOUNTAIN WAY' --  2 crashes
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NE 40TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'NE 40TH ST AT 132ND' and OfficialSt = 'NE 40TH ST' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NEWCASTLE GOLF CLUB RD' where INDEXED_PRIMARY_TRAFFICWAY = 'NEWCASTLE GOLF CLUB' and OfficialSt = 'NEWCASTLE GOLF CLUB RD' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NORTHUP WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'NORTHUP WAY/NE 20TH' and OfficialSt = 'NORTHUP WAY' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'RICHARDS RD' where INDEXED_PRIMARY_TRAFFICWAY = 'RICHARDS RD SE' and OfficialSt = 'RICHARDS RD' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE 37TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'SE 35TH PL' and OfficialSt = 'SE 37TH ST' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE 36TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'SE 38TH ST' and OfficialSt = 'SE 36TH ST' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '127TH AVE SE' where INDEXED_PRIMARY_TRAFFICWAY = 'SE 62ND ST' and OfficialSt = '127TH AVE SE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE 26TH ST' where INDEXED_PRIMARY_TRAFFICWAY = 'KAMBER RD' and OfficialSt = 'SE 26TH ST' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'RICHARDS RD' where INDEXED_PRIMARY_TRAFFICWAY = 'LAKE HILLS CONNECTOR' and OfficialSt = 'RICHARDS RD' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '145TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = '140TH AVE SE' and OfficialSt = '145TH PL SE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '142ND AVE SE' where INDEXED_PRIMARY_TRAFFICWAY = '142ND PL SE' and OfficialSt = '142ND AVE SE' --  1 crash 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '136TH AVE SE' where INDEXED_PRIMARY_TRAFFICWAY = '136TH AVE NE' and OfficialSt = '136TH AVE SE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'NE SPRING BLVD' where INDEXED_PRIMARY_TRAFFICWAY = '136TH PL NE AT NE SP' and OfficialSt = 'NE SPRING BLVD' --  1 crash 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '113TH PL SE' where INDEXED_PRIMARY_TRAFFICWAY = '112TH PL SE' and OfficialSt = '113TH PL SE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '114TH AVE NE' where INDEXED_PRIMARY_TRAFFICWAY = '114TH AVE SE' and OfficialSt = '114TH AVE NE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = '116TH AVE NE' where INDEXED_PRIMARY_TRAFFICWAY = '116TH AVE NE OVERLAK' and OfficialSt = '116TH AVE NE' --  1 crash 
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'LAKE WASHINGTON BLVD SE' where INDEXED_PRIMARY_TRAFFICWAY = '118TH AVE SE' and OfficialSt = 'LAKE WASHINGTON BLVD SE' --  1 crash
update #temp4 set INDEXED_PRIMARY_TRAFFICWAY_updated = 'SE WOLVERINE WAY' where INDEXED_PRIMARY_TRAFFICWAY = 'WOLVERINE WAY' and OfficialSt = 'SE WOLVERINE WAY' --  1 crash




-- =============================================================================================================
-- STEP 4: Re-run Step 1 but using updated street name for matching. 
-- =============================================================================================================


-- get list of crashes which snap to a road name using the lowest near_rank value. 
SELECT distinct a.*
--into #temp5 
FROM #temp4 a
join #temp4 b on a.INDEXED_PRIMARY_TRAFFICWAY_updated = b.OfficialSt and a.objectid = b.objectid 


-- 2,085 distinct crash reports (21 reports missing) 
select a.*
--into #temp6
from #temp5 a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp5 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst, INDEXED_PRIMARY_TRAFFICWAY_updated 
	from #temp5 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, INDEXED_PRIMARY_TRAFFICWAY, officialst, INDEXED_PRIMARY_TRAFFICWAY_updated 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


select * from #temp6 where near_rank = '1' order  by near_rank  --1,653


-- 7 crash reports are still not 
select * 
from [temp].[dbo].[All_roads_Bellevue_CrashData_NearTable] a 
where not exists (
	select DISTINCT IN_FID   
	from #temp6 b 
	where a.in_fid = b.in_fid 
) order by in_fid, near_rank 




-- NEXT STEP:  export #temp6 and import it into ArcGIS

select * from #temp6 


 
