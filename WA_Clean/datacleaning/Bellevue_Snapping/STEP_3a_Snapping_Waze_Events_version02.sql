
-- =============================================================================================================
-- STEP 1: Get distinct list of Waze reports which snap to a road segment with the same name of the lowest rank. 
-- =============================================================================================================

-- 73,344 total distinct Waze events 
select distinct IN_FID from [temp].[dbo].[Waze_NearTable]



SELECT distinct a.*
--into #temp 
FROM [temp].[dbo].[Waze_NearTable] a
join [temp].[dbo].[Waze_NearTable] b on UPPER(a.street) = b.OfficialSt and a.objectid = b.objectid 


-- 65,404 distinct Waze reports 
select a.*
--into #temp2
from #temp a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, sdc_uuid, street, rdseg_id, officialst 
	from #temp 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, sdc_uuid, street, rdseg_id, officialst 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


-- list of Waze reports which snap to a road segment with the same name and the rank = 1 (e.g. closest segment) --> 64,182
select * from #temp2 where near_rank = '1' order  by near_rank  


-- 1,057 accidents snapped to a segment within 50ft and directly match the road name  
select * from #temp2 where alert_type = 'ACCIDENT' 



-- =============================================================================================================
-- STEP 2: Get distinct list of Waze reports which do not snap to a road segment with the same name. 
-- =============================================================================================================

-- 24,850 
SELECT distinct a.*
--into #temp3 
FROM [temp].[dbo].[Waze_NearTable] a
join [temp].[dbo].[Waze_NearTable] b on UPPER(a.street) <> b.OfficialSt and a.objectid = b.objectid 



-- 7,945 distinct Waze reports do not match to a segment with the same street name 
select DISTINCT a.IN_FID  
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 



-- =============================================================================================================
-- STEP 3: Get a list of unmatched road names of snapped Waze reports on the Bellevue road network. 
-- =============================================================================================================

-- find unmatch streets --> bring this into ARCGIS for review 
select a.from_x, a.from_y, a.SDC_uuid, a.street, a.rdseg_id,  a.officialst 
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 



-- IN ARCGIS: 
--------------
-- Review the unmatched streets to determine if they are the same streets, 
-- but different names, or if they are in fact, different streets.  
-- If they are the same streets, change the street name in the street_updated column (see below).  


-- find unmatch streets --> 218 pairs (8,027 total rows)
select a.*
--into #temp4
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 
AND STREET <> 'NA' AND OFFICIALST <> 'UNNAMED'


-- add a new field called 'street_updated' and populate it with the values from the Waze street column. 
alter table #temp4 add street_updated varchar(50) 
update #temp4 set street_updated = UPPER(street) 



-- get count of unique pairs --> 218 
select a.street, a.officialst, COUNT(*) as COUNT 
from #temp4 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 
AND STREET <> 'NA' AND OFFICIALST <> 'UNNAMED'
GROUP BY a.STREET, a.OFFICIALST 
order by COUNT(*) DESC  



-- update the street name if the segments are the same, and if the count is > 100, or it the segments have almost identical names. Additionally, all accidents are snapped.  
update #temp4 set street_updated = 'LAKE HILLS CN' where street = UPPER('Lake Hills Connector') and OfficialSt = 'LAKE HILLS CN' -- 1,614 Waze reports 
update #temp4 set street_updated = 'NE BELLEVUE-REDMOND RD' where street = UPPER('Bel-Red Rd') and OfficialSt = 'NE BELLEVUE-REDMOND RD' -- 1,017 Waze reports 
update #temp4 set street_updated = 'LAKE WASHINGTON BLVD SE' where street = UPPER('118th Ave SE') and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 959 Waze reports 
update #temp4 set street_updated = 'SE COAL CREEK PKWY' where street = UPPER('Coal Creek Pkwy SE') and OfficialSt = 'SE COAL CREEK PKWY' -- 825 Waze reports 
update #temp4 set street_updated = '156TH AVE SE' where street = UPPER('156th Ave SE') and OfficialSt = '156TH AVE NE' -- 722 Waze reports 
update #temp4 set street_updated = '112TH AVE SE' where street = UPPER('112th Ave SE') and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 533 Waze reports 
update #temp4 set street_updated = 'LAKE HILLS CN' where street = UPPER('116th Ave SE') and OfficialSt = 'LAKE HILLS CN' -- 373 Waze reports 
update #temp4 set street_updated = '142ND AVE SE' where street = UPPER('142nd Pl SE') and OfficialSt = '142ND AVE SE' -- 197 Waze reports 
update #temp4 set street_updated = '128TH AVE SE' where street = UPPER('Factoria Blvd SE') and OfficialSt = '128TH AVE SE' -- 134 Waze reports 
update #temp4 set street_updated = 'W LAKE SAMMAMISH PKWY NE' where street = UPPER('W Lake Sammamish Pkwy SE') and OfficialSt = 'W LAKE SAMMAMISH PKWY NE' -- 112 Waze reports 
update #temp4 set street_updated = 'SE 36TH ST' where street = UPPER('SE 38th St') and OfficialSt = 'SE 36TH ST' -- 86 Waze reports 
update #temp4 set street_updated = '128TH AVE SE' where street = UPPER('Richards Rd') and OfficialSt = '128TH AVE SE' -- 79 Waze reports 
update #temp4 set street_updated = 'SE 35TH PL' where street = UPPER('SE 34th St') and OfficialSt = 'SE 35TH PL' -- 70 Waze reports 
update #temp4 set street_updated = 'LAKEMONT BLVD SE' where street = UPPER('Newcastle Golf Club Rd') and OfficialSt = 'LAKEMONT BLVD SE' -- 62 Waze reports 
update #temp4 set street_updated = 'SE COAL CREEK RD' where street = UPPER('Coal Creek Rd') and OfficialSt = 'SE COAL CREEK RD' -- 42 Waze reports 
update #temp4 set street_updated = '164TH AVE SE' where street = UPPER('164th Way SE') and OfficialSt = '164TH AVE SE' -- 15 Waze reports 
update #temp4 set street_updated = 'LAKE HEIGHTS ST' where street = UPPER('Lake Heights St SE') and OfficialSt = 'LAKE HEIGHTS ST' -- 15 Waze reports 
update #temp4 set street_updated = 'KELSEY CREEK RD SE' where street = UPPER('Kelsy Creek Rd') and OfficialSt = 'KELSEY CREEK RD SE' -- 9 Waze reports 
update #temp4 set street_updated = 'SNOQUALMIE RIVER RD SE' where street = UPPER('Snoqualmie River Rd') and OfficialSt = 'SNOQUALMIE RIVER RD SE' -- 7 Waze reports 
update #temp4 set street_updated = 'NE 8TH ST' where street = UPPER('NE 8th Ave') and OfficialSt = 'NE 8TH ST' -- 7 Waze reports 
update #temp4 set street_updated = '148TH PL SE' where street = UPPER('148th Ave SE') and OfficialSt = '148TH PL SE' -- 7 Waze reports 
update #temp4 set street_updated = '148TH AVE NE' where street = UPPER('148th Ave SE') and OfficialSt = '148TH AVE NE' -- 6 Waze reports 




SELECT distinct a.*, b.street_updated
--into #temp5
FROM [temp].[dbo].[Waze_NearTable] a
full join #temp4 b on a.objectid = b.objectid 


update #temp5 set street_updated = UPPER(street) where street_updated is null  -- 99,218


select * from #temp5 order by OBJECTID 


-- =============================================================================================================
-- STEP 4: Re-run Step 1 but using updated street name for matching. 
-- =============================================================================================================


-- get distinct list of Waze reports which snap to a road name using the lowest near_rank value. 
SELECT distinct a.*
--into #temp6 
FROM #temp5 a
join #temp5 b on UPPER(a.street_updated) = b.OfficialSt and a.objectid = b.objectid 


-- 70,226 distinct Waze reports 
select a.*
--into #temp7
from #temp6 a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp6 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst, street_updated 
	from #temp6 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst, street_updated 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


-- 68,949 reports 
select * from #temp7 where near_rank = '1' order  by near_rank  


-- 1,176 accidents snapped to a segment within 50ft and directly match the road name 
select * from #temp7 where alert_type = 'ACCIDENT'


-- =============================================================================================================
-- STEP 5: Re-run Step 2 but with updated street name for matching.  
-- =============================================================================================================

-- 2,388 
SELECT distinct a.*
--into #temp8
FROM #temp4 a
join #temp4 b on UPPER(a.street_updated) <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp8 order by objectid 


-- 1,331 distinct Waze reports do not match to a segment with the same street name 
select DISTINCT a.IN_FID  
from #temp8 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp7 b 
	where a.in_fid = b.in_fid 
) and (STREET <> 'NA' or OFFICIALST <> 'UNNAMED')



-- NEXT STEP:  export #temp7 and import it into ArcGIS 



