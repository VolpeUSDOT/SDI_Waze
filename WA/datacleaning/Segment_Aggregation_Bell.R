
# Grid agg scrip start
# Aggregation of Waze and Bellevue crash data by segment and hour

rm(list= ls())
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(tidyverse)
library(doParallel)

codeloc <- ifelse(grep('Flynn', normalizePath('~/')),
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze")

source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
output.loc <- file.path(data.loc, "Segments")

setwd(data.loc)

# Set up months to do ----

segfiles <- dir(output.loc)[grep('^WazeSegTimeList_', dir(output.loc))]

avail.months = substr(unlist(
  lapply(strsplit(segfiles, "_"), function(x) x[2])), 1, 7)

# Look for already completed months and skip those
done_files <- dir(output.loc)[grep("WazeSegTime_", dir(output.loc))]
done.months <- substr(unlist(lapply(strsplit(done_files, "_"), function(x) x[2])), 1, 7)
todo.months = sort(avail.months)[!avail.months %in% done.months]

# Start aggregation by month ----

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

writeLines(c(""), paste0("SegAgg_log.txt"))    

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
  # j = "2018-08"  
  sink(paste0("SegAgg_log.txt"), append=TRUE)
  
  cat(paste(Sys.time()), j, "\n")                                                  
  load(file.path(output.loc, paste0("WazeSegTimeList_", j, ".RData"))) # includes both Waze (link.seg.waze) and crash data (link.seg.crash) 

  # Aggregate: new data frame will have one row per segment, per hour, per day.
  # Response variable column: count of unique crash events in this segment, within this time window. 
  # Predictor variable columns: median values for the numeric characteristics: report rating, confidence..
  # Counts for the number of Waze events of each alert_type and sub_type, inside this segment at this time.
  # TODO: include neighboring segments?
  
  # !!!!!!!!!!!!!!!!!!!!!!! Dan stopped here 2019-03-29 ----
  
  StartTime <- Sys.time()
  waze.seg <- Waze.seg.time %>%
    mutate(Date = as.POSIXct(Date)) %>%
    group_by(RDSEG_ID,
             year = format(Date, "%Y"), day = format(Date, "%j"), hour = formatC(Hour, width = 2, flag = '0'), weekday = format(Date, "%u")) %>%
    summarize(
      uniqueWazeEvents= n_distinct(SDC_uuid), # number of unique Waze events.
      
      nWazeAccident = n_distinct(SDC_uuid[alert_type=="ACCIDENT"]),
      nWazeJam = n_distinct(SDC_uuid[alert_type=="JAM"]),
      nWazeRoadClosed = n_distinct(SDC_uuid[alert_type=="ROAD_CLOSED"]),
      nWazeWeatherOrHazard = n_distinct(SDC_uuid[alert_type=="WEATHERHAZARD"]),
      
      nHazardOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD"|sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"
                                           |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"
                                           |sub_type=="HAZARD_ON_ROAD_ICE"|sub_type=="HAZARD_ON_ROAD_LANE_CLOSED"
                                           |sub_type=="HAZARD_ON_ROAD_OBJECT"|sub_type=="HAZARD_ON_ROAD_POT_HOLE"
                                           |sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nHazardOnShoulder = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_SHOULDER"|sub_type=="HAZARD_ON_SHOULDER_ANIMALS"
                                               |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"
                                               |sub_type=="HAZARD_ON_SHOULDER_MISSING_SIGN"]),
      nHazardWeather = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER"|sub_type=="HAZARD_WEATHER_FLOOD"
                                            |sub_type=="HAZARD_WEATHER_FOG"|sub_type=="HAZARD_WEATHER_HAIL"
                                            |sub_type=="HAZARD_WEATHER_MONSOON"|sub_type=="HAZARD_WEATHER_FREEZING_RAIN"
                                            |sub_type=="HAZARD_WEATHER_HEAVY_SNOW"|sub_type=="HAZARD_WEATHER_HEAVY_RAIN"]),
      
      nWazeAccidentMajor = n_distinct(SDC_uuid[sub_type=="ACCIDENT_MAJOR"]),
      nWazeAccidentMinor = n_distinct(SDC_uuid[sub_type=="ACCIDENT_MINOR"]),
      
      nWazeHazardCarStoppedRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"]),
      nWazeHazardCarStoppedShoulder = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"]),
      nWazeHazardConstruction = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"]),
      nWazeHazardObjectOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_OBJECT"]),
      nWazeHazardPotholeOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_POT_HOLE"]),
      nWazeHazardRoadKillOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nWazeJamModerate = n_distinct(SDC_uuid[sub_type=="JAM_MODERATE_TRAFFIC"]),
      nWazeJamHeavy = n_distinct(SDC_uuid[sub_type=="JAM_HEAVY_TRAFFIC"]),
      nWazeJamStandStill = n_distinct(SDC_uuid[sub_type=="JAM_STAND_STILL_TRAFFIC"]),
      nWazeWeatherFlood = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER_FLOOD"]),
      nWazeWeatherFog = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER_FOG"]),
      nWazeHazardIceRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_ICE"]),
      
      nWazeRT3 = n_distinct(SDC_uuid[road_type=="3"]),
      nWazeRT4 = n_distinct(SDC_uuid[road_type=="4"]),
      nWazeRT6 = n_distinct(SDC_uuid[road_type=="6"]),
      nWazeRT7 = n_distinct(SDC_uuid[road_type=="7"]),
      nWazeRT2 = n_distinct(SDC_uuid[road_type=="2"]),
      nWazeRT0 = n_distinct(SDC_uuid[road_type=="0"]),
      nWazeRT1 = n_distinct(SDC_uuid[road_type=="1"]),
      nWazeRT20 = n_distinct(SDC_uuid[road_type=="20"]),
      nWazeRT17 = n_distinct(SDC_uuid[road_type=="17"]),
      
      medLastRepRate = median(last.reportRating), # median is going to be affected if Waze.seg.time table has duplicates 
      medLastConf = median(last.confidence),
      medLastReliab = median(last.reliability),
      
      medMagVar = median(magvar),
      nMagVar330to30 = n_distinct(SDC_uuid[magvar>= 330 & magvar<30]),
      nMagVar30to60 = n_distinct(SDC_uuid[magvar>= 60 & magvar<120]),
      nMagVar90to180 = n_distinct(SDC_uuid[magvar>= 120 & magvar<180]),
      nMagVar180to240 = n_distinct(SDC_uuid[magvar>= 180 & magvar<240]),
      nMagVar240to360 = n_distinct(SDC_uuid[magvar>= 240 & magvar<360])) 
  
  
  #Compute grid counts for TN data -- update all of these to the TN data variables
  # Add accident severity counts by grid cell 
  # names(crash.df)
  tn.seg <- 
    crash.df %>%
    group_by(RDSEG_ID.TN, year = format(date, "%Y"), day = format(date, "%j"), hour = format(date, "%H")) %>%
    summarize(
      uniqueTNreports= n_distinct(MstrRecNbrTxt),
      
      nTNInjuryFatal = n_distinct(MstrRecNbrTxt[NbrFatalitiesNmb > 0]),
      nTNInjury = n_distinct(MstrRecNbrTxt[NbrInjuredNmb > 0]),
      nTNAlcoholInd = n_distinct(MstrRecNbrTxt[AlcoholInd == "Y"]),
      nTNIntersectionInd = n_distinct(MstrRecNbrTxt[IntersectionInd == "Y"])
    ) 
  
  #Merge TN crash counts to waze counts by hexagons
  names(waze.seg)
  names(tn.seg)
  colnames(tn.seg)[1] <- "RDSEG_ID"
  
  wazeTime.tn.seg <- full_join(waze.seg, tn.seg, by = c("RDSEG_ID", "year", "day", "hour")) %>%
    mutate_all(funs(replace(., is.na(.), 0)))
  # Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or TN crashes in the grid cell at that hour)
  
  #Add columns containing data for neighboring grid cells 
  names(wazeTime.tn.seg)
  
  #Accident counts for neighboring cells
  nWazeAcc <- wazeTime.tn.seg %>%
    ungroup()%>%
    select(RDSEG_ID, day, hour, nWazeAccident)%>%
    rename(nWazeAcc_neighbor = nWazeAccident)
  
  #Jam counts for neighboring cells
  nWazeJam <- wazeTime.tn.seg %>%
    ungroup()%>%
    select(RDSEG_ID, day, hour, nWazeJam)%>%
    rename(nWazeJam_neighbor = nWazeJam)
  

  # test process - look at value for highest count in nWazeAcc_NW column (10)
  # t=filter(wazeTime.tn.seg_NW_N_NE_SW_S_SE, RDSEG_ID=="EG-53" & day=="141" & hour=="15")
  # t #10 - this matches, test more
  
  wazeTime.tn.segAll <- wazeTime.tn.seg_NW_N_NE_SW_S_SE %>%
    mutate_all(funs(replace(., is.na(.), 0)))
  # Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the neighbor grid cell at that hour)
  
  # Update time variable 
  hextimeChar <- paste(paste(wazeTime.tn.segAll$year, wazeTime.tn.segAll$day, sep = "-"), wazeTime.tn.segAll$hour, sep=" ")
  wazeTime.tn.segAll$hextime <- hextimeChar #strptime(hextimeChar, "%Y:%j:%H", tz = use.tz)
  
  class(wazeTime.tn.segAll) <- "data.frame" # POSIX date/time not supported for grouped tbl
  
  fn = paste("WazeTimeEdtHexAll_", j,"_", g, ".RData", sep="")
  
  save(list="wazeTime.tn.segAll", file = file.path(temp.outputdir, fn))
  
  # Copy to S3
  system(paste("aws s3 cp",
               file.path(temp.outputdir, fn),
               file.path(teambucket, state, fn)))
  
  EndTime <- Sys.time()-StartTime
  cat(j, 'completed', round(EndTime, 2), attr(EndTime, 'units'), '\n')
  
} # End month aggregation loop ----


stopCluster(cl)

} # end grid loop

# Load weather data -- Only need to assign to a Date
load(file.path(data.loc, "Weather","Prepared_Bellevue_Wx_2018.RData")) # the file name wx.grd.day
# format day
wx.grd.day$day <- as.Date(wx.grd.day$day)
