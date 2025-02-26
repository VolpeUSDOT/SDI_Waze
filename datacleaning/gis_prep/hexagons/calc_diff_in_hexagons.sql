
-- NOTE: when importing make sure the following fields are:
	-- HOUR = NUMERIC
	-- DATE = DATE
	-- JAMS = NUMERIC 
	-- ACCIDENTS = NUMERIC 


SELECT 
	a.GRID_ID, a.hour, 
	b.nWazeJam as event_jams, a.nWazeJam as noevent_jams, (b.nWazeJam - a.nWazeJam) as diff_jams, 
	b.nWazeAccident as event_accidents, a.nWazeAccident as noevent_accidents, (b.nWazeAccident - a.nWazeAccident) as diff_accidents
FROM [temp].[dbo].[DT_All_MileBuffer_MD_AprilToSept_2017] a 
join (
	SELECT GRID_ID, date, hour, nWazeJam, nWazeAccident
	FROM [temp].[dbo].[DT_All_MileBuffer_MD_AprilToSept_2017] 
	WHERE date = '2017-09-10 00:00:00.000'
	and Grid_In_Model = 'Yes'
	and buffer_3 = '1'
	and EventType = 'Football'
) b on a.GRID_ID = b.GRID_ID and a.hour = b.hour
where a.date = '2017-09-17 00:00:00.000'
and a.Grid_In_Model = 'Yes' 
and a.buffer_3 = '1'
and a.EventType = 'NoEvent'
order by GRID_ID, hour