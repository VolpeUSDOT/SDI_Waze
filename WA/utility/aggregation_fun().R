# Functions to group data by hour window (e.g., 4 hours)
# Waze and Bellevue crash we need to aggregate by both hour window and segment
# Weather we need to aggregate by day and segment
# Other variables we need to aggregate by segments

group_by_Waze_Crash <- function(table, ... ) {
  table %>% group_by(.dots = ...) %>% 
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
      nCrashSeriousInjury = sum(nCrashSeriousInjury),
      nCrashKSI = sum(nCrashKSI),
      nCrashInjury = sum(nCrashInjury),
      nCrashPDO = sum(nCrashPDO),
      nCrashWorkzone = sum(nCrashWorkzone)
    )
}

# Weather, only by day and segments
group_by_Weather <- function(table, ... ) {
  table %>% group_by(.dots = ...) %>% 
    summarise(
      PRCP = mean(PRCP, na.rm = T), # Should not be any NA, just to be safe adding na.rm = T
      TMIN = mean(TMIN, na.rm = T),                        
      TMAX = mean(TMAX, na.rm = T),
      SNOW = mean(SNOW, na.rm = T)
    )
}

# function to aggregate when specifying the dataset, segment, and time variable
agg_fun <- function(w.all, t_var) {
  
  # Waze and crash need to match hour and segments
  grp_cols = c("RDSEG_ID", "year", t_var, "grp_name")
  dots = lapply(grp_cols, as.symbol)
  w.all.4hr <- group_by_Waze_Crash(w.all, .dots = dots)
  
  # weather need to match day and segments
  grp_cols = c("RDSEG_ID", "year", t_var)
  dots = lapply(grp_cols, as.symbol)
  wx.grd.day <- group_by_Weather(w.all, .dots = dots)
  w.all.4hr <-left_join(w.all.4hr, wx.grd.day, by = c('RDSEG_ID', 'year', t_var))
  
  # all other variables need to match segments
  seg_only_var <- names(w.all)[c(1, 57:94)]
  seg.only.data <- unique(w.all[, seg_only_var])
  
  w.all.4hr <- left_join(w.all.4hr, seg.only.data, by = 'RDSEG_ID')
  class(w.all.4hr) <- "data.frame" # POSIX date/time not supported for grouped tbl
  
  # some other variables
  w.all.4hr$grp_hr <- ifelse(w.all.4hr$grp_name == "Early AM", "04",
                             ifelse(w.all.4hr$grp_name == "AM Peak", "08",
                                    ifelse(w.all.4hr$grp_name == "Mid-day", "12",
                                           ifelse(w.all.4hr$grp_name == "PM Peak", "16",
                                                  ifelse(w.all.4hr$grp_name == "Evening", "20",
                                                         ifelse(w.all.4hr$grp_name == "Mid-night", "00", NA))))))
  
  w.all.4hr$biCrash <- ifelse(w.all.4hr$uniqueCrashreports > 0, 1, 0)
  
  if (t_var == 'day') {
       w.all.4hr <- w.all.4hr %>% mutate(segtime = paste(paste(year, day, sep = "-"), grp_hr, sep=" "),
                                          time_hr = as.POSIXct(segtime, '%Y-%j %H', tz = 'America/Los_Angeles'),
                                          date = as.Date(time_hr, format = '%Y-%j %H', tz = 'America/Los_Angeles'),
                                          month = as.Date(cut(date, breaks = "month"), tz = 'America/Los_Angeles'),
                                          weekday = as.factor(weekdays(date))
    )
     
  }

  w.all.4hr
}
