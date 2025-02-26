
--SELECT *
--FROM [CDANFACTS].[odslite].[vCrashFact]
--WHERE CrashStudy = 'EDT' 
--AND CrashState IN ('CONNECTICUT', 'MARYLAND', 'UTAH', 'VIRGINIA')
--AND CrashDate BETWEEN '2017-04-01 00:00:00.000' AND '2018-08-31 00:00:00.000'
--ORDER BY CrashState, CrashDate, CrashCaseID



SELECT *
FROM [CDANFACTS].[odslite].[vCrashFact]
WHERE CrashStudy = 'EDT' 
AND CrashState IN ('Connecticut', 'Maryland', 'Utah', 'Virginia')
AND StudyYear = '2018'
AND GPSLat IS NOT NULL
AND GPSLong IS NOT NULL 
ORDER BY CrashState, CrashDate, CrashCaseID

-- total = 728,516 reports 
-- where LAT and LON are NOT NULL = 324,639
-- where LAT or LONG is NULL = 403,877