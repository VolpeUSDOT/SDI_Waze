
USE temp
GO 

-- NOTE: when importing files, make sure VOLUMN in factors table and SUM_proportional_aadt in TOTAL AADT TABLE are numeric 
-- VOLUME: DataPercision = 13, DataScale = 11
-- SUM_proportional_aadt: DataPercision: 18, DataScale = 0 (doesnt matter because it is a whole number) 



SELECT TOP 10 *
  FROM [temp].[dbo].[non_conditional_census_method_2016_normalized]


SELECT TOP 10 *
  --FROM [temp].[dbo].[CT_total_aadt_by_grid_fc_urban]
  --FROM [temp].[dbo].[MD_total_aadt_by_grid_fc_urban]
  --FROM [temp].[dbo].[UT_total_aadt_by_grid_fc_urban]
  --FROM [temp].[dbo].[VA_total_aadt_by_grid_fc_urban]
  --FROM [temp].[dbo].[TN_1sqmile_hexagons_total_aadt_by_grid_fc_urban] -- TN hexagons 
  FROM [temp].[dbo].[TN_01dd_fishnet_total_aadt_by_grid_fc_urban] -- TN fishnet grid 


-- seperate out functional class column to two new columns in vmt table 
ALTER TABLE non_conditional_census_method_2016_normalized
ADD URBAN_RURAL VARCHAR(50)

ALTER TABLE non_conditional_census_method_2016_normalized
ADD FC VARCHAR(50)


-- populate the new tables with the proper values 
UPDATE non_conditional_census_method_2016_normalized
SET URBAN_RURAL = LEFT(FUNC_CLASS, 1)

UPDATE non_conditional_census_method_2016_normalized
SET URBAN_RURAL = 'RURAL' WHERE URBAN_RURAL = 'R'

UPDATE non_conditional_census_method_2016_normalized
SET URBAN_RURAL = 'URBAN' WHERE URBAN_RURAL = 'U'

UPDATE non_conditional_census_method_2016_normalized
SET FC = RIGHT(FUNC_CLASS, 1)



-- join VMT factors table and proportional aadt values tables 
SELECT 
	b.GRID_ID, a.MONTH, a.DAYOFWEEK, a.HOUR, b.F_SYSTEM_VN, a.URBAN_RURAL, a.VOLUME, b.SUM_PROPORTIONAL_AADT 
INTO #temp  
FROM non_conditional_census_method_2016_normalized a
JOIN (
	SELECT GRID_ID, F_SYSTEM_VN, URBAN_RURAL, SUM_PROPORTIONAL_AADT 
	--FROM CT_total_aadt_by_grid_fc_urban --CONNECTICUT 
	--FROM MD_total_aadt_by_grid_fc_urban --MARYLAND
	--FROM UT_total_aadt_by_grid_fc_urban --UTAH
	--FROM VA_total_aadt_by_grid_fc_urban --VIRGINIA
	--FROM TN_1sqmile_hexagons_total_aadt_by_grid_fc_urban -- TN hexagons 
	FROM TN_01dd_fishnet_total_aadt_by_grid_fc_urban -- TN fishnet grid 
) b ON a.FC = b.F_SYSTEM_VN AND a.URBAN_RURAL = b.URBAN_RURAL 
--WHERE a.STATE = 'CT'
--WHERE a.STATE = 'MD'
--WHERE a.STATE = 'UT'
--WHERE a.STATE = 'VA'
WHERE a.STATE = 'TN'
AND a.VEH_CLASS = 'ALL'
ORDER BY b.GRID_ID


SELECT TOP 10 * FROM #temp
WHERE F_SYSTEM_VN = 4 AND URBAN_RURAL = 'RURAL'
--DROP TABLE #temp


ALTER TABLE #temp 
ADD HOURLY_VMT NUMERIC(15, 6)


-- NOTE: have to run these for individual functional classes 
-- for both the urban and rural areas because of memory/size 
UPDATE #temp
SET HOURLY_VMT = SUM_PROPORTIONAL_AADT * VOLUME
WHERE F_SYSTEM_VN = 7 AND URBAN_RURAL = 'URBAN'

UPDATE #temp
SET HOURLY_VMT = SUM_PROPORTIONAL_AADT * VOLUME
WHERE F_SYSTEM_VN = 7 AND URBAN_RURAL = 'RURAL'


SELECT * 
--INTO CT_total_aadt_by_grid_fc_urban_VMTfactored --CONNECTICUT
--INTO MD_total_aadt_by_grid_fc_urban_VMTfactored --MARYLAND 
--INTO UT_total_aadt_by_grid_fc_urban_VMTfactored --UTAH
--INTO VA_total_aadt_by_grid_fc_urban_VMTfactored --VIRGINIA
--INTO TN_1sqmile_hexagons_total_aadt_by_grid_fc_urban_VMTfactored --TN hexagons 
INTO TN_01dd_fishnet_total_aadt_by_grid_fc_urban_VMTfactored -- TN fishnet grid
FROM #temp 