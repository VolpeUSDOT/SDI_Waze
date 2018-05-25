# Aggregation of Waze and EDT by grid cell
# Goal: create a gridded data set where grid cell contain the count of 
# Start from UrbanArea_overlay.R


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(tidyverse)
library(lubridate)
library(utils)
library(doParallel)
library(foreach)

output.loc <- "~/tempout"
localdir <- "/home/daniel/workingdata/" # full path for readOGR
edtdir <- normalizePath(file.path(localdir, "EDT"))
wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"),
                  stringsAsFactors = F)


#Set parameters for data to process
HEXSIZE = 1# c("1", "4", "05")#[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids


  codedir <- "~/SDI_Waze" 
  wazemonthdir <- "~/tempin"
  localdir <- "~/workingdata"
  outputdir <- "~/tempout"


source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function
setwd(localdir)

# Loop through months of available merged data
avail.months = unique(substr(dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))], 
                      start = 17,
                      stop = 18))

ONLOCAL = F

if(ONLOCAL) { temp.outputdir = tempdir() # for temporary storage 
} else {
  temp.outputdir = "~/agg_out"
}

todo.months = avail.months[c(2:7)]

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

for(SIZE in HEXSIZE){ # Start hex size loop

writeLines(c(""), paste0(SIZE, "_log.txt"))    

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
  
  sink(paste0(SIZE, "_log.txt"), append=TRUE)
  
  cat(paste(Sys.time()), j, "\n")                                                           
  
  load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_",SIZE,"mi","_MD.RData"))) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
  
  # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
  # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
  link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  
  # <><><><><><><><><><><><><><><><><><><><>
  
  #Read in the data frame of all Grid IDs by day of year and time of day in each month of data (subet to all grid IDs with Waze OR EDT data)
  load(file.path(paste(outputdir, "/WazeHexTimeList_", j,"_",SIZE,"mi",".RData",sep="")))
  
  # aggregate: new data frame will have one row per cell, per hour, per day.
  # Response variable column: count of unique EDT events matching Waze events in this cell, within this time window. 
  # Predictor variable columns: median values for the numeric characteristics: report rating, confidence..
  # Counts for the number of waze events of each type and subtype, inside this grid cell at this time.
  # The same, but for each of the neighboring grid cells (N, NE, SE, S, SW, NW). 
  # counts for roadType, 11 columns: length(unique(link.waze.edt$roadType[!is.na(link.waze.edt$roadType)]))
  
  #summarize counts of Waze events in each hexagon and EDT matches to the Waze events (could be in neighboring hexagon)
  names(Waze.hex.time)
  
  StartTime <- Sys.time()
  waze.hex <- Waze.hex.time %>%
    group_by(GRID_ID, GRID_ID_NW, GRID_ID_N, GRID_ID_NE, GRID_ID_SW, GRID_ID_S, GRID_ID_SE,
             day = format(GridDayHour, "%j"), hour = format(GridDayHour, "%H"), weekday = format(GridDayHour, "%u")) %>%
    summarize(
      nWazeRowsInMatch = n(), #includes duplicates that match more than one EDT report (don't use in model)
      uniqWazeEvents= n_distinct(uuid.waze),
      nMatchEDT_buffer = n_distinct(CrashCaseID[match=="M"]),
    
      nMatchEDT_buffer_Acc = n_distinct(CrashCaseID[match=="M" & type=="ACCIDENT"]),
      nMatchEDT_buffer_Jam = n_distinct(CrashCaseID[match=="M" & type=="JAM"]),
      nMatchEDT_buffer_RoadClosed = n_distinct(CrashCaseID[match=="M" & type=="ROAD_CLOSED"]),
      nMatchEDT_buffer_WorH = n_distinct(CrashCaseID[match=="M"& type=="WEATHERHAZARD"]),
      
      nMatchWaze_buffer = n_distinct(uuid.waze[match=="M"]),
      nNoMatchWaze_buffer = n_distinct(uuid.waze[match=="W"]),
      
      nWazeAccident = n_distinct(uuid.waze[type=="ACCIDENT"]),
      nWazeJam = n_distinct(uuid.waze[type=="JAM"]),
      nWazeRoadClosed = n_distinct(uuid.waze[type=="ROAD_CLOSED"]),
      nWazeWeatherOrHazard = n_distinct(uuid.waze[type=="WEATHERHAZARD"]),
      
      nHazardOnRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD"|subtype=="HAZARD_ON_ROAD_CAR_STOPPED"
                                           |subtype=="HAZARD_ON_ROAD_CAR_STOPPED"|subtype=="HAZARD_ON_ROAD_CONSTRUCTION"
                                           |subtype=="HAZARD_ON_ROAD_ICE"|subtype=="HAZARD_ON_ROAD_LANE_CLOSED"
                                           |subtype=="HAZARD_ON_ROAD_OBJECT"|subtype=="HAZARD_ON_ROAD_POT_HOLE"
                                           |subtype=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nHazardOnShoulder = n_distinct(uuid.waze[subtype=="HAZARD_ON_SHOULDER"|subtype=="HAZARD_ON_SHOULDER_ANIMALS"
                                               |subtype=="HAZARD_ON_ROAD_CAR_STOPPED"|subtype=="HAZARD_ON_SHOULDER_CAR_STOPPED"
                                               |subtype=="HAZARD_ON_SHOULDER_MISSING_SIGN"]),
      nHazardWeather = n_distinct(uuid.waze[subtype=="HAZARD_WEATHER"|subtype=="HAZARD_WEATHER_FLOOD"
                                            |subtype=="HAZARD_WEATHER_FOG"|subtype=="HAZARD_WEATHER_HAIL"
                                            |subtype=="HAZARD_WEATHER_MONSOON"|subtype=="HAZARD_WEATHER_FREEZING_RAIN"
                                            |subtype=="HAZARD_WEATHER_HEAVY_SNOW"|subtype=="HAZARD_WEATHER_HEAVY_RAIN"]),

      nWazeAccidentMajor = n_distinct(uuid.waze[subtype=="ACCIDENT_MAJOR"]),
      nWazeAccidentMinor = n_distinct(uuid.waze[subtype=="ACCIDENT_MINOR"]),

      nWazeHazardCarStoppedRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_CAR_STOPPED"]),
      nWazeHazardCarStoppedShoulder = n_distinct(uuid.waze[subtype=="HAZARD_ON_SHOULDER_CAR_STOPPED"]),
      nWazeHazardConstruction = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_CONSTRUCTION"]),
      nWazeHazardObjectOnRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_OBJECT"]),
      nWazeHazardPotholeOnRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_POT_HOLE"]),
      nWazeHazardRoadKillOnRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nWazeJamModerate = n_distinct(uuid.waze[subtype=="JAM_MODERATE_TRAFFIC"]),
      nWazeJamHeavy = n_distinct(uuid.waze[subtype=="JAM_HEAVY_TRAFFIC"]),
      nWazeJamStandStill = n_distinct(uuid.waze[subtype=="JAM_STAND_STILL_TRAFFIC"]),
      nWazeWeatherFlood = n_distinct(uuid.waze[subtype=="HAZARD_WEATHER_FLOOD"]),
      nWazeWeatherFog = n_distinct(uuid.waze[subtype=="HAZARD_WEATHER_FOG"]),
      nWazeHazardIceRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_ICE"]),
      
      nWazeRT3 = n_distinct(uuid.waze[roadType=="3"]),
      nWazeRT4 = n_distinct(uuid.waze[roadType=="4"]),
      nWazeRT6 = n_distinct(uuid.waze[roadType=="6"]),
      nWazeRT7 = n_distinct(uuid.waze[roadType=="7"]),
      nWazeRT2 = n_distinct(uuid.waze[roadType=="2"]),
      nWazeRT0 = n_distinct(uuid.waze[roadType=="0"]),
      nWazeRT1 = n_distinct(uuid.waze[roadType=="1"]),
      nWazeRT20 = n_distinct(uuid.waze[roadType=="20"]),
      nWazeRT17 = n_distinct(uuid.waze[roadType=="17"]),
      
      medLastRepRate = median(last.reportRating),
      medLastConf = median(last.confidence),
      medLastReliab = median(last.reliability),
      
      medMagVar = median(magvar),
      nMagVar330to30 = n_distinct(uuid.waze[magvar>= 330 & magvar<30]),
      nMagVar30to60 = n_distinct(uuid.waze[magvar>= 60 & magvar<120]),
      nMagVar90to180 = n_distinct(uuid.waze[magvar>= 120 & magvar<180]),
      nMagVar180to240 = n_distinct(uuid.waze[magvar>= 180 & magvar<240]),
      nMagVar240to360 = n_distinct(uuid.waze[magvar>= 240 & magvar<360])) 

  EndTime <- Sys.time()-StartTime
  EndTime
  
  #Compute grid counts for EDT data
  #Add accident severity counts by grid cell 
  names(edt.df)
  edt.hex <- 
    edt.df %>%
    group_by(GRID_ID.edt, day = format(CrashDate_Local, "%j"), hour = format(CrashDate_Local, "%H")) %>%
    summarize(
      uniqEDTreports= n_distinct(CrashCaseID),
      nEDTMaxDamDisabling = n_distinct(CrashCaseID[MaxExtentofDamage=="Disabling Damage"]),
      nEDTMaxDamFunctional = n_distinct(CrashCaseID[MaxExtentofDamage==" Functional Damage"]),
      nEDTMaxDamMinor = n_distinct(CrashCaseID[MaxExtentofDamage=="Minor Damage"]),
      nEDTMaxDamNone = n_distinct(CrashCaseID[MaxExtentofDamage==" No Damage"]),
      nEDTMaxDamNotReported = n_distinct(CrashCaseID[MaxExtentofDamage=="Not Reported"]),
      nEDTMaxDamUnknown = n_distinct(CrashCaseID[MaxExtentofDamage=="Unknown"]),
      
      nEDTInjuryFatal = n_distinct(CrashCaseID[MaxExtentofDamage=="Fatal Injury (K)"]),
      nEDTInjuryNone = n_distinct(CrashCaseID[MaxExtentofDamage=="No Apparent Injury (O)"]),
      nEDTInjuryPossible = n_distinct(CrashCaseID[MaxExtentofDamage=="Possible Injury (C)"]),
      nEDTInjurySuspMinor = n_distinct(CrashCaseID[MaxExtentofDamage=="Suspected Minor Injury(B)"]),
      nEDTInjurySuspSerious = n_distinct(CrashCaseID[MaxExtentofDamage=="Suspected Serious Injury(A)"])
      ) 
  
  #Merge EDT counts to waze counts by hexagons
  names(waze.hex)
  names(edt.hex)
  colnames(edt.hex)[1] <- "GRID_ID"
  
  wazeTime.edt.hex <- full_join(waze.hex, edt.hex, by = c("GRID_ID", "day", "hour")) %>%
    mutate_all(funs(replace(., is.na(.), 0)))
  #Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the grid cell at that hour)
  
  #Add columns containing data for neighboring grid cells 
  names(wazeTime.edt.hex)

  #Accident counts for neighboring cells
  nWazeAcc <- wazeTime.edt.hex %>%
    ungroup()%>%
    select(GRID_ID, day, hour, nWazeAccident)%>%
    rename(nWazeAcc_neighbor=nWazeAccident)
  
  wazeTime.edt.hex_NW <- left_join(wazeTime.edt.hex, nWazeAcc, by = c("GRID_ID_NW"="GRID_ID","day"="day", "hour"="hour")) %>%
    rename(nWazeAcc_NW=nWazeAcc_neighbor)
  wazeTime.edt.hex_NW_N <- left_join(wazeTime.edt.hex_NW, nWazeAcc, by = c("GRID_ID_N"="GRID_ID","day"="day", "hour"="hour")) %>%
    rename(nWazeAcc_N=nWazeAcc_neighbor)
  wazeTime.edt.hex_NW_N_NE <- left_join(wazeTime.edt.hex_NW_N, nWazeAcc, by = c("GRID_ID_NE"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeAcc_NE=nWazeAcc_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW <- left_join(wazeTime.edt.hex_NW_N_NE, nWazeAcc, by = c("GRID_ID_SW"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeAcc_SW=nWazeAcc_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW_S <- left_join(wazeTime.edt.hex_NW_N_NE_SW, nWazeAcc, by = c("GRID_ID_S"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeAcc_S=nWazeAcc_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW_S_SE <- left_join(wazeTime.edt.hex_NW_N_NE_SW_S, nWazeAcc, by = c("GRID_ID_SE"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeAcc_SE=nWazeAcc_neighbor)
  
  #Jam counts for neighboring cells
  nWazeJam <- wazeTime.edt.hex %>%
    ungroup()%>%
    select(GRID_ID, day, hour, nWazeJam)%>%
    rename(nWazeJam_neighbor=nWazeJam)
  
  wazeTime.edt.hex <- wazeTime.edt.hex_NW_N_NE_SW_S_SE #edit names so we're not overwriting earlier df
  wazeTime.edt.hex_NW <- left_join(wazeTime.edt.hex, nWazeJam, by = c("GRID_ID_NW"="GRID_ID","day"="day", "hour"="hour")) %>%
    rename(nWazeJam_NW=nWazeJam_neighbor)
  wazeTime.edt.hex_NW_N <- left_join(wazeTime.edt.hex_NW, nWazeJam, by = c("GRID_ID_N"="GRID_ID","day"="day", "hour"="hour")) %>%
    rename(nWazeJam_N=nWazeJam_neighbor)
  wazeTime.edt.hex_NW_N_NE <- left_join(wazeTime.edt.hex_NW_N, nWazeJam, by = c("GRID_ID_NE"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeJam_NE=nWazeJam_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW <- left_join(wazeTime.edt.hex_NW_N_NE, nWazeJam, by = c("GRID_ID_SW"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeJam_SW=nWazeJam_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW_S <- left_join(wazeTime.edt.hex_NW_N_NE_SW, nWazeJam, by = c("GRID_ID_S"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeJam_S=nWazeJam_neighbor)
  wazeTime.edt.hex_NW_N_NE_SW_S_SE <- left_join(wazeTime.edt.hex_NW_N_NE_SW_S, nWazeJam, by = c("GRID_ID_SE"="GRID_ID","day"="day", "hour"="hour"))%>%
    rename(nWazeJam_SE=nWazeJam_neighbor)
  
  #test process - look at value for highest count in nWazeAcc_NW column (10)
  t=filter(wazeTime.edt.hex_NW_N_NE_SW_S_SE, GRID_ID=="EG-53" & day=="141" & hour=="15")
  t #10 - this matches, test more
  
  wazeTime.edt.hexAll <- wazeTime.edt.hex_NW_N_NE_SW_S_SE%>%
    mutate_all(funs(replace(., is.na(.), 0)))
  #Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the neighbor grid cell at that hour)
  
  #Update time variable 
  hextimeChar <- paste(wazeTime.edt.hexAll$day,wazeTime.edt.hexAll$hour,sep=":")
  wazeTime.edt.hexAll$hextime <- strptime(hextimeChar, "%j:%H", tz=)

  save(list="wazeTime.edt.hexAll", file = paste(temp.outputdir, "/WazeTimeEdtHexAll_", j,"_",SIZE,"mi",".RData",sep=""))

} # End month aggregation loop ----

if(ONLOCAL) { movefiles(dir(temp.outputdir)[grep("Hex", dir(temp.outputdir))], temp.outputdir, outputdir) }

}

stopCluster(cl)
##########################################################################################################
##########################################################################################################

# Check with plots

CHECKPLOT = T

library(rgdal)

gdaldir = "/home/dflynn-volpe/workingdata/" # Requires full path, no alias

if(CHECKPLOT){  
  
  co <- rgdal::readOGR(gdaldir, layer = "cb_2015_us_county_500k")
  
  # maryland FIPS = 24
  md.co <- co[co$STATEFP == 24,]
  
  pdf("Checking_Grid_agg_newID.pdf", width = 11, height = 8)
  for(SIZE in HEXSIZE){
  # Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, 
  hex <- rgdal::readOGR(gdaldir, layer = paste0("MD_hexagons_", SIZE, "mi_newExtent_newGRIDID"))
  
  hex2 <- spTransform(hex, proj4string(md.co))
  plot(hex2)
  plot(md.co, add = T, col = "red")
  
  load(paste0(temp.outputdir, "/WazeTimeEdtHexAll_04_", SIZE,"mi",".RData"))
  
  class(wazeTime.edt.hexAll) <- "data.frame"
  
  # Join back to hex2, just check unique grid IDs for plotting
  w.t <- wazeTime.edt.hexAll[!duplicated(wazeTime.edt.hexAll$GRID_ID),]
  
  h.w <- hex2[hex2$GRID_ID %in% w.t$GRID_ID,]
  plot(h.w, col = "blue", add = T)
  }
  dev.off()
}