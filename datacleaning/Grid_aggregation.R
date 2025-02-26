# Aggregation of Waze and EDT by grid cell
# Goal: create a gridded data set where grid cell contain the count of 
# Follows Hex_UA_Overlay_SDC.R


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(tidyverse)
library(lubridate)
library(utils)
library(doParallel)
library(foreach)
library(circular)

output.loc <- "~/agg_out"
user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata/") # full path for readOGR
edtdir <- normalizePath(file.path(localdir, "EDT"))
wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary
wazemonthdir <- "~/workingdata/Overlay" 


teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"
         ),
                  stringsAsFactors = F)


#Set parameters for data to process
HEXSIZE = 1# c("1", "4", "05")#[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

source(file.path(codeloc, "utility/wazefunctions.R")) # for movefiles() function; only need for local, obsolete on SDC
setwd(localdir)

temp.outputdir = "~/agg_out"


# start state loop ----
for(state in states){ # state = "CT"
  
  #Loop through months of available merged data
  mergefiles <- dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))]
  statemergefiles <- mergefiles[grep(paste0(state, ".RData"), mergefiles)]
  
  avail.months = unique(substr(statemergefiles, 
                               start = 17,
                               stop = 23))
  
  # Look for already completed months and skip those
  tlfiles <- dir(temp.outputdir)[grep("WazeTimeEdtHexAll_", dir(temp.outputdir))]
  state.tlfiles <- tlfiles[grep(state, tlfiles)]
  done.months <- unlist(lapply(strsplit(state.tlfiles, "_"), function(x) x[[2]])) 
  
  # Manually set done.months to just the 2017 months, re-do all 2018 the same way
  done.months <- done.months[grep('2017', done.months)]
  
  todo.months = avail.months #sort(avail.months[!avail.months %in% done.months])
  
  use.tz <- tzs$tz[tzs$states == state]
  
  
  cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
  registerDoParallel(cl)
  
  writeLines(c(""), paste0(state, "_log.txt"))    
  
  foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils", "circular")) %dopar% {
    # j = '2018-02'  
    sink(paste0(state, "_log.txt"), append=TRUE)
    
    cat(paste(Sys.time()), j, "\n")                                                           
    
    load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_", state, ".RData"))) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
    
    
    # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
    # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
    link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = use.tz)
    edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = use.tz)
    
    # <><><><><><><><><><><><><><><><><><><><>
    
    #Read in the data frame of all Grid IDs by day of year and time of day in each month of data (subset to all grid IDs with Waze OR EDT data)
 
    
    load(file.path(temp.outputdir, paste0("WazeHexTimeList_", j,"_",HEXSIZE,"mi_",state,".RData")))
    
    # aggregate: new data frame will have one row per cell, per hour, per day.
    # Response variable column: count of unique EDT events matching Waze events in this cell, within this time window. 
    # Predictor variable columns: median values for the numeric characteristics: report rating, confidence..
    # Counts for the number of waze events of each alert_type and sub_type, inside this grid cell at this time.
    # The same, but for each of the neighboring grid cells (N, NE, SE, S, SW, NW). 
    # counts for road_type, 11 columns: length(unique(link.waze.edt$road_type[!is.na(link.waze.edt$road_type)]))
    
    #summarize counts of Waze events in each hexagon and EDT matches to the Waze events (could be in neighboring hexagon)
    # names(Waze.hex.time)
    
    StartTime <- Sys.time()
    waze.hex <- Waze.hex.time %>%
      mutate(MagVar.circ = circular(magvar, units = "degrees", template = "geographics")) %>%
      group_by(GRID_ID, GRID_ID_NW, GRID_ID_N, GRID_ID_NE, GRID_ID_SW, GRID_ID_S, GRID_ID_SE,
               day = format(GridDayHour, "%j"), hour = format(GridDayHour, "%H"), weekday = format(GridDayHour, "%u")) %>%
      summarize(
        nWazeRowsInMatch = n(), #includes duplicates that match more than one EDT report (don't use in model)
        uniqueWazeEvents= n_distinct(uuid.waze),
        nMatchEDT_buffer = n_distinct(CrashCaseID[match=="M"]),
      
        nMatchEDT_buffer_Acc = n_distinct(CrashCaseID[match=="M" & alert_type=="ACCIDENT"]),
        nMatchEDT_buffer_Jam = n_distinct(CrashCaseID[match=="M" & alert_type=="JAM"]),
        nMatchEDT_buffer_RoadClosed = n_distinct(CrashCaseID[match=="M" & alert_type=="ROAD_CLOSED"]),
        nMatchEDT_buffer_WorH = n_distinct(CrashCaseID[match=="M"& alert_type=="WEATHERHAZARD"]),
        
        nMatchWaze_buffer = n_distinct(uuid.waze[match=="M"]),
        nNoMatchWaze_buffer = n_distinct(uuid.waze[match=="W"]),
        
        nWazeAccident = n_distinct(uuid.waze[alert_type=="ACCIDENT"]),
        nWazeJam = n_distinct(uuid.waze[alert_type=="JAM"]),
        nWazeRoadClosed = n_distinct(uuid.waze[alert_type=="ROAD_CLOSED"]),
        nWazeWeatherOrHazard = n_distinct(uuid.waze[alert_type=="WEATHERHAZARD"]),
        
        nHazardOnRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD"|sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"
                                             |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"
                                             |sub_type=="HAZARD_ON_ROAD_ICE"|sub_type=="HAZARD_ON_ROAD_LANE_CLOSED"
                                             |sub_type=="HAZARD_ON_ROAD_OBJECT"|sub_type=="HAZARD_ON_ROAD_POT_HOLE"
                                             |sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
        nHazardOnShoulder = n_distinct(uuid.waze[sub_type=="HAZARD_ON_SHOULDER"|sub_type=="HAZARD_ON_SHOULDER_ANIMALS"
                                                 |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"
                                                 |sub_type=="HAZARD_ON_SHOULDER_MISSING_SIGN"]),
        nHazardWeather = n_distinct(uuid.waze[sub_type=="HAZARD_WEATHER"|sub_type=="HAZARD_WEATHER_FLOOD"
                                              |sub_type=="HAZARD_WEATHER_FOG"|sub_type=="HAZARD_WEATHER_HAIL"
                                              |sub_type=="HAZARD_WEATHER_MONSOON"|sub_type=="HAZARD_WEATHER_FREEZING_RAIN"
                                              |sub_type=="HAZARD_WEATHER_HEAVY_SNOW"|sub_type=="HAZARD_WEATHER_HEAVY_RAIN"]),
  
        nWazeAccidentMajor = n_distinct(uuid.waze[sub_type=="ACCIDENT_MAJOR"]),
        nWazeAccidentMinor = n_distinct(uuid.waze[sub_type=="ACCIDENT_MINOR"]),
  
        nWazeHazardCarStoppedRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"]),
        nWazeHazardCarStoppedShoulder = n_distinct(uuid.waze[sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"]),
        nWazeHazardConstruction = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"]),
        nWazeHazardObjectOnRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_OBJECT"]),
        nWazeHazardPotholeOnRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_POT_HOLE"]),
        nWazeHazardRoadKillOnRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
        nWazeJamModerate = n_distinct(uuid.waze[sub_type=="JAM_MODERATE_TRAFFIC"]),
        nWazeJamHeavy = n_distinct(uuid.waze[sub_type=="JAM_HEAVY_TRAFFIC"]),
        nWazeJamStandStill = n_distinct(uuid.waze[sub_type=="JAM_STAND_STILL_TRAFFIC"]),
        nWazeWeatherFlood = n_distinct(uuid.waze[sub_type=="HAZARD_WEATHER_FLOOD"]),
        nWazeWeatherFog = n_distinct(uuid.waze[sub_type=="HAZARD_WEATHER_FOG"]),
        nWazeHazardIceRoad = n_distinct(uuid.waze[sub_type=="HAZARD_ON_ROAD_ICE"]),
        
        #Omit road closures from counts (not reported by users)
        nWazeRT3 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="3"]),
        nWazeRT4 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="4"]),
        nWazeRT6 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="6"]),
        nWazeRT7 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="7"]),
        nWazeRT2 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="2"]),
        nWazeRT0 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="0"]),
        nWazeRT1 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="1"]),
        nWazeRT20 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="20"]),
        nWazeRT17 = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & road_type=="17"]),
        
        medLastRepRate = median(last.reportRating),
        medLastConf = median(last.confidence),
        medLastReliab = median(last.reliability),
        
        meanCirMagVar = mean.circular(MagVar.circ[alert_type!="ROAD_CLOSED"]),
        medCirMagVar = median.circular(MagVar.circ[alert_type!="ROAD_CLOSED"]),
        
        nMagVar330to30N = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 330 | magvar<30]),
        nMagVar30to90NE = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 30 & magvar<90]),
        nMagVar90to150SE = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 90 & magvar<150]),
        nMagVar150to210S = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 150 & magvar<210]),
        nMagVar210to270SW = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 210 & magvar<270]),
        nMagVar270to330NW = n_distinct(uuid.waze[alert_type!="ROAD_CLOSED" & magvar>= 270 & magvar<330]),  
    
        UA_Cluster = 1*any(Waze_UA_Type == 'C' & !is.na(Waze_UA_Type)),
        UA_Urban = 1*any(Waze_UA_Type == 'U' & !is.na(Waze_UA_Type)),
        UA_Rural = 1*any(is.na(Waze_UA_Type))
        ) # end Waze aggregation 
  
    EndTime <- Sys.time()-StartTime
    EndTime
    
    #Compute grid counts for EDT data
    #Add accident severity counts by grid cell 
    # names(edt.df)
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
    
    # test process - look at value for highest count in nWazeAcc_NW column (10)
    # t=filter(wazeTime.edt.hex_NW_N_NE_SW_S_SE, GRID_ID=="EG-53" & day=="141" & hour=="15")
    # t #10 - this matches, test more
     
    wazeTime.edt.hexAll <- wazeTime.edt.hex_NW_N_NE_SW_S_SE %>%
      mutate_all(funs(replace(., is.na(.), 0)))
    #Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the neighbor grid cell at that hour)
    
    #Update time variable 
    hextimeChar <- paste(wazeTime.edt.hexAll$day,wazeTime.edt.hexAll$hour,sep=":")
    wazeTime.edt.hexAll$hextime <- strptime(hextimeChar, "%j:%H", tz=)
  
    
    class(wazeTime.edt.hexAll) <- "data.frame" # POSIX date/time not supported for grouped tbl
    
    fn = paste("WazeTimeEdtHexAll_", j,"_",HEXSIZE,"mi_", state, ".RData", sep="")
    
    save(list="wazeTime.edt.hexAll", file = file.path(temp.outputdir, fn))

    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, fn),
                 file.path(teambucket, state, fn)))
    
  } # End month aggregation loop ----
  
  
  stopCluster(cl)

} # end state loop

##########################################################################################################
##########################################################################################################

# move any old files to S3 (should not be needed any more, S3 copy command is part of loop now.)

SCRATCH = F
if(SCRATCH){
aggfiles <- dir(temp.outputdir) # consists of both the TimeList and HexAll files for each state
for(state in states){
  state.aggfiles <- aggfiles[grep(state, aggfiles)]
  for(i in state.aggfiles){
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, i),
                 file.path(teambucket, state, i)))
  }
}
} # end scratch

# Check with plots 

CHECKPLOT = T


library(rgdal)
library(maps) # for state.fips

if(CHECKPLOT){  
  
  co <- rgdal::readOGR(file.path(localdir, 'census'), layer = "cb_2017_us_county_500k")

  FIPS = state.fips[state.fips$abb %in% states,]
  FIPS = FIPS[!duplicated(FIPS$abb),]
  FIPS$fips = formatC(FIPS$fips, width = 2, flag = "0")  
  
  pdf(file.path("Figures", "Checking_Grid_agg_Multistate.pdf"), width = 11, height = 8)
  for(state in states){ # state = 'UT'

  months = c("2018-02", "2018-11") # just a few for example

  state.co <- co[co$STATEFP == FIPS[FIPS$abb==state,"fips"],]
    
  # Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, 
    hex = readOGR(file.path(localdir, "Hex", state), layer = paste0(state, "_hexagons_1mi_neighbors"))
  
  hex2 <- spTransform(hex, proj4string(state.co))
  
  for(j in months){
      plot(hex2, main = paste(state, j))
    
      plot(state.co, add = T, col = "red")
      
      fn = paste0("WazeTimeEdtHexAll_", j, "_1mi_", state, ".RData")
      
      load(file.path(temp.outputdir, fn))
      
      class(wazeTime.edt.hexAll) <- "data.frame"
      
      # Join back to hex2, just check unique grid IDs for plotting
      w.t <- wazeTime.edt.hexAll[!duplicated(wazeTime.edt.hexAll$GRID_ID),]
      
      h.w <- hex2[hex2$GRID_ID %in% w.t$GRID_ID,]
      plot(h.w, col = "blue", add = T)
    }
  }
  dev.off()
}
