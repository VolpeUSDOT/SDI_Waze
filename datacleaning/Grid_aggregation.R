# Aggregation of Waze and EDT by grid cell
# Goal: create a gridded data set where grid cell contain the count of Waze and EDT records and other auxiliary data. 
# Start from UrbanArea_overlay.R

#Notes on data coverage
#For data completeness context, there are 33,184 1-mile hexagon grid cells in MD (including border cells) and ~720 Day-hours. 
#Complete observations for all grid-cells/day-hours would consist of ~23.9 million rows 


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(lubridate)
library(utils)

#Set parameters for data to process
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids


#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                    "WazeEDT Agg",HEXSIZE,"mile Rdata Input")

#Sudderth drive
codedir <- "~/GitHub/SDI_Waze"  #CONNECT TO VPN FIRST
wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "S:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- paste0("S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                    "WazeEDT Agg",HEXSIZE,"mile Rdata Input")


source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function
setwd(wazedir)

# Loop through months of available merged data
avail.months = unique(substr(dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))], 
                      start = 17,
                      stop = 18))

temp.outputdir = tempdir()# for temporary storage 

todo.months = avail.months#[c(5:7)]

for(j in todo.months){ #j = "04"
  
  load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_",HEXSIZE,"mi","_MD.RData"))) 
  # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells (link.waze.edt not used in this script)
  
  # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
  # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
  link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  
  # <><><><><><><><><><><><><><><><><><><><>
  
  #Read in the data frame of all Grid IDs by day of year and time of day in each month of data with Waze events OR EDT reports 
  #For April, the df has ~1.29 million observations (in 4,534 unique grid IDs out of ~33K total in MD, and in 717 unique DayHour periods)
  load(file.path(paste(outputdir, "/WazeHexTimeList_", j,"_",HEXSIZE,"mi",".RData",sep="")))
  
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
  #To only include rows with a Waze "signal", rows with no Waze accidents or weather/hazards are removed prior to model fitting in RandomForest scripts
  
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
  
  #test process - look at value for highest count in nWazeAcc_NW column and confirm match
  GridID.maxWazeAcc = which(wazeTime.edt.hex_NW_N_NE_SW_S_SE$nWazeAccident==max(wazeTime.edt.hex_NW_N_NE_SW_S_SE$nWazeAccident))
  print(paste("Max Number waze accidents = ", wazeTime.edt.hex_NW_N_NE_SW_S_SE$nWazeAccident[GridID.maxWazeAcc]))
  GridID.maxWazeAcc.naccSouth = wazeTime.edt.hex_NW_N_NE_SW_S_SE$nWazeAcc_S[GridID.maxWazeAcc]
  print(paste("Number waze accidents South Max = ", GridID.maxWazeAcc.naccSouth))
  GridID.south = wazeTime.edt.hex_NW_N_NE_SW_S_SE$GRID_ID_S[GridID.maxWazeAcc]
  day = wazeTime.edt.hex_NW_N_NE_SW_S_SE$day[GridID.maxWazeAcc]
  hour = wazeTime.edt.hex_NW_N_NE_SW_S_SE$hour[GridID.maxWazeAcc]
  GridID.south.nWazeAcc = wazeTime.edt.hex_NW_N_NE_SW_S_SE$nWazeAccident[
    wazeTime.edt.hex_NW_N_NE_SW_S_SE$GRID_ID==GridID.south & 
      wazeTime.edt.hex_NW_N_NE_SW_S_SE$day==day & 
      wazeTime.edt.hex_NW_N_NE_SW_S_SE$hour==hour]
  Test.match = GridID.maxWazeAcc.naccSouth == GridID.south.nWazeAcc
  print(paste("Number Waze Accidents South of Max Matches Grid Cell Count = ", Test.match))
  
  wazeTime.edt.hexAll <- wazeTime.edt.hex_NW_N_NE_SW_S_SE%>%
    mutate_all(funs(replace(., is.na(.), 0)))
  #Replace NA with zero (for the neighbor grid counts here, 0 means there were no reported Waze events of the column type or EDT crashes at that hour)
  
  #Update time variable 
  hextimeChar <- paste(wazeTime.edt.hexAll$day,wazeTime.edt.hexAll$hour,sep=":")
  wazeTime.edt.hexAll$hextime <- strptime(hextimeChar, "%j:%H", tz=)

  save(list="wazeTime.edt.hexAll", file = paste(temp.outputdir, "/WazeTimeEdtHexAll_", j,"_",HEXSIZE,"mi",".RData",sep=""))

} # End month aggregation loop ----

movefiles(dir(temp.outputdir)[grep("Hex", dir(temp.outputdir))], temp.outputdir, outputdir)

##########################################################################################################
##########################################################################################################