# Functions to group data by hour window (e.g., 4 hours)
# Waze and Bellevue crash we need to aggregate by both hour window and segment
# Weather we need to aggregate by day and segment
# Other variables we need to aggregate by segments

group_by_Waze_Crash <- function(table, ... ) {
  table %>% group_by(.dots = lazyeval::lazy_dots(...)) %>% 
    summarise(
# Waze columns
      uniqueWazeEvents = sum(uniqueWazeEvents), # number of unique Waze events.

      nWazeAccident = sum(nWazeAccident),
      nWazeJam = sum(nWazeJam),
      nWazeRoadClosed = sum(nWazeRoadClosed),
      nWazeWeatherOrHazard = sum(nWazeWeatherOrHazard),
      
      nHazardOnRoad = sum(nHazardOnRoad),
      nHazardOnShoulder = sum(nHazardOnShoulder),
      nHazardWeather = sum(nHazardWeather),
      
      nWazeAccidentMajor = sum(nWazeAccidentMajor),
      nWazeAccidentMinor = sum(nWazeAccidentMinor),
      
      nWazeHazardCarStoppedRoad = sum(nWazeHazardCarStoppedRoad),
      nWazeHazardCarStoppedShoulder = sum(nWazeHazardCarStoppedShoulder),
      nWazeHazardConstruction = sum(nWazeHazardConstruction),
      nWazeHazardObjectOnRoad = sum(nWazeHazardObjectOnRoad),
      nWazeHazardPotholeOnRoad = sum(nWazeHazardPotholeOnRoad),
      nWazeHazardRoadKillOnRoad = sum(nWazeHazardRoadKillOnRoad),
      nWazeJamModerate = sum(nWazeJamModerate),
      nWazeJamHeavy = sum(nWazeJamHeavy),
      nWazeJamStandStill = sum(nWazeJamStandStill),
      nWazeWeatherFlood = sum(nWazeWeatherFlood),
      nWazeWeatherFog = sum(nWazeWeatherFog),
      nWazeHazardIceRoad = sum(nWazeHazardIceRoad),
      
      nWazeRT3 = sum(nWazeRT3),
      nWazeRT4 = sum(nWazeRT4),
      nWazeRT6 = sum(nWazeRT6),
      nWazeRT7 = sum(nWazeRT7),
      nWazeRT2 = sum(nWazeRT2),
      nWazeRT0 = sum(nWazeRT0),
      nWazeRT1 = sum(nWazeRT1),
      nWazeRT20 = sum(nWazeRT20),
      nWazeRT17 = sum(nWazeRT17),
      
      medMagVar = median(medMagVar), # Median direction of travel for that road segment for that hour.
      nMagVar330to30 = sum(nMagVar330to30),
      nMagVar30to60 = sum(nMagVar30to60),
      nMagVar90to180 = sum(nMagVar90to180),
      nMagVar180to240 = sum(nMagVar180to240),
      nMagVar240to360 = sum(nMagVar240to360),
  
# Crash columns
      uniqueCrashreports = sum(uniqueCrashreports),
      
      nCrashInjuryFatal = sum(nCrashInjuryFatal),
      nCrashInjury = sum(nCrashInjury),
      nCrashPDO = sum(nCrashPDO),
      nCrashWorkzone = sum(nCrashWorkzone)
    )
}

# Weather, only by day and segments
group_by_weather <- function(table, ... ) {
  table %>% group_by_(.dots = lazyeval::lazy_dots(...)) %>% 
    summarise(
      PRCP = mean(PRCP),
      TMIN = mean(TMIN),                        
      TMAX = mean(TMAX),
      SNOW = mean(SNOW),
      mo = unique(mo)
    )
}
