

-- =============================================================================================================
-- STEP 1: Get distinct list of Waze reports which snap to a road segment with the same name of the lowest rank. 
-- =============================================================================================================

SELECT distinct a.*
--into #temp 
FROM [temp].[dbo].[WA_Bellevue_Prep_Export_NearTable] a
join [temp].[dbo].[WA_Bellevue_Prep_Export_NearTable] b on UPPER(a.street) = b.OfficialSt and a.objectid = b.objectid 


-- 109,088 distinct Waze reports 
select a.*
--into #temp2
from #temp a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst 
	from #temp 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


-- list of Waze reports which snap to a road segment with the same name and the rank = 1 (e.g. closest segment) 
select * from #temp2 where near_rank = '1' order  by near_rank  



-- =============================================================================================================
-- STEP 2: Get distinct list of Waze reports which do not snap to a road segment with the same name. 
-- =============================================================================================================

SELECT distinct a.*
--into #temp3 
FROM [temp].[dbo].[WA_Bellevue_Prep_Export_NearTable] a
join [temp].[dbo].[WA_Bellevue_Prep_Export_NearTable] b on UPPER(a.street) <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp2 order by objectid 


-- 13,325 distinct Waze reports do not match to a segment with the same street name 
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

-- find unmatch streets 
select a.street, a.officialst, COUNT(*) as COUNT 
from #temp3 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp2 b 
	where a.in_fid = b.in_fid 
) 
AND STREET <> 'NA' AND OFFICIALST <> 'UNNAMED'
GROUP BY a.STREET, a.OFFICIALST 
order by COUNT(*) DESC  


-- IN ARCGIS: 
--------------
-- Review the unmatched streets to determine if they are the same streets, 
-- but different names, or if they are in fact, different streets.  
-- If they are the same streets, change the street name in the street_updated column (see below).  


-- create temp table with the original near table upload. 
select * 
--into #temp4 
from [temp].[dbo].[WA_Bellevue_Prep_Export_NearTable]


-- add a new field called 'street_updated' and populate it with the values from the Waze street column. 
alter table #temp4 add street_updated varchar(50) 
update #temp4 set street_updated = UPPER(street) 


-- update the street name if the segments are the same, and if the count is > 100, or it the segments have almost identical names. 
update #temp4 set street_updated = 'LAKE HILLS CN' where street = UPPER('Lake Hills Connector') and OfficialSt = 'LAKE HILLS CN' -- 2,702 Waze reports 
update #temp4 set street_updated = 'NE BELLEVUE-REDMOND RD' where street = UPPER('Bel-Red Rd') and OfficialSt = 'NE BELLEVUE-REDMOND RD' -- 1,776 Waze reports 
update #temp4 set street_updated = '118TH AVE SE' where street = UPPER('118th Ave SE') and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 1,570 Waze reports 
update #temp4 set street_updated = 'SE COAL CREEK PKWY' where street = UPPER('Coal Creek Pkwy SE') and OfficialSt = 'SE COAL CREEK PKWY' -- 1,378 Waze reports 
update #temp4 set street_updated = '156TH AVE SE' where street = UPPER('156th Ave SE') and OfficialSt = '156TH AVE NE' -- 1,274 Waze reports 
update #temp4 set street_updated = '112TH AVE SE' where street = UPPER('112th Ave SE') and OfficialSt = 'LAKE WASHINGTON BLVD SE' -- 766 Waze reports 
update #temp4 set street_updated = 'LAKE HILLS CN' where street = UPPER('116th Ave SE') and OfficialSt = 'LAKE HILLS CN' -- 643 Waze reports 
update #temp4 set street_updated = '142ND PL SE' where street = UPPER('142nd Pl SE') and OfficialSt = '142ND AVE SE' -- 337 Waze reports 
update #temp4 set street_updated = 'FACTORIA BLVD SE' where street = UPPER('Factoria Blvd SE') and OfficialSt = '128TH AVE SE' -- 224 Waze reports 
update #temp4 set street_updated = 'W LAKE SAMMAMISH PKWY SE' where street = UPPER('W Lake Sammamish Pkwy SE') and OfficialSt = 'W LAKE SAMMAMISH PKWY NE' -- 186 Waze reports 
update #temp4 set street_updated = 'RICHARDS RD' where street = UPPER('Richards Rd') and OfficialSt = '128TH AVE SE' -- 149 Waze reports 
update #temp4 set street_updated = 'LAKEMONT BLVD SE' where street = UPPER('Newcastle Golf Club Rd') and OfficialSt = 'LAKEMONT BLVD SE' -- 133 Waze reports 
update #temp4 set street_updated = 'COAL CREEK RD' where street = UPPER('Coal Creek Rd') and OfficialSt = 'SE COAL CREEK RD' -- 73 Waze reports 
update #temp4 set street_updated = 'KELSY CREEK RD' where street = UPPER('Kelsy Creek Rd') and OfficialSt = 'KELSEY CREEK RD SE' -- 17 Waze reports 



-- =============================================================================================================
-- STEP 4: Re-run Step 1 but using updated street name for matching. 
-- =============================================================================================================


-- get distinct list of Waze reports which snap to a road name using the lowest near_rank value. 
SELECT distinct a.*
--into #temp5 
FROM #temp4 a
join #temp4 b on UPPER(a.street_updated) = b.OfficialSt and a.objectid = b.objectid 


-- 114,614 distinct Waze reports 
select a.*
--into #temp6
from #temp5 a 
join (
	select in_fid, min(near_rank) as near_rank 
	from #temp5 
	group by in_fid 
) b on a.in_fid = b.in_fid and a.NEAR_RANK = b.near_rank 
join ( 
	select in_fid, min(near_fid) as near_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst, street_updated 
	from #temp5 
	group by in_fid, near_dist, near_rank, from_x, from_y, near_x, near_y, street, officialst, street_updated 
) c on a.in_fid = c.in_fid and a.near_fid = c.near_fid
order by objectid 


select * from #temp6 where near_rank = '1' order  by near_rank  




-- =============================================================================================================
-- STEP 5: Re-run Step 2 but with updated street name for matching.  
-- =============================================================================================================

SELECT distinct a.*
--into #temp7
FROM #temp4 a
join #temp4 b on UPPER(a.street_updated) <> b.OfficialSt and a.objectid = b.objectid 


select * from #temp7 order by objectid 


-- 7,799 distinct Waze reports do not match to a segment with the same street name 
-- 7,737 distinct Waze reports do not match to a segement with the same street name (this does not include NAs or unnamed streets) 
select DISTINCT a.IN_FID  
from #temp7 a 
where not exists (
	select DISTINCT IN_FID   
	from #temp6 b 
	where a.in_fid = b.in_fid 
) and (STREET <> 'NA' or OFFICIALST <> 'UNNAMED')




-- NEXT STEP:  export #temp6 and import it into ArcGIS 

