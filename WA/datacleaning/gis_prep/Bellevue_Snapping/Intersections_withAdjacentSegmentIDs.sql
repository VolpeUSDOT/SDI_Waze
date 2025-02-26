


SELECT streetsegm , end1_intid as endID 
--INTO #temp
FROM [temp].[dbo].[RoadNetwork_Jurisdiction_withIntersections_FullCrash] 
WHERE end1_intid <> 0
union 
SELECT streetsegm , end2_intid as endID 
FROM [temp].[dbo].[RoadNetwork_Jurisdiction_withIntersections_FullCrash] 
WHERE end2_intid <> 0
ORDER BY endID



select endid, count(*)
from #temp 
group by endid 
order by COUNT(*) desc  



select * from #temp order by endid 


-- get row id for each intersection id 
select 
	main.endid, main.streetsegm, 
	ROW_NUMBER() over (partition by main.endid order by main.streetsegm) as row_id
into #temp2
from #temp main 
order by endid, row_id 


select * from #temp2 order by endid, row_id 




select endid, 
	max(case when row_id = 1 then streetsegm else null end) as streetsegm_1, 
	max(case when row_id = 2 then streetsegm else null end) as streetsegm_2, 
	max(case when row_id = 3 then streetsegm else null end) as streetsegm_3, 
	max(case when row_id = 4 then streetsegm else null end) as streetsegm_4, 
	max(case when row_id = 5 then streetsegm else null end) as streetsegm_5, 
	max(case when row_id = 6 then streetsegm else null end) as streetsegm_6
from #temp2 
group by endid 
order by endid 



select endid, COUNT(*) 
from #temp2
group by endid 
having COUNT(*) = 1
