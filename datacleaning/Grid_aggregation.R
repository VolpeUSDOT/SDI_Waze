# Aggregation of Waze and EDT by grid cell
# Goal: create a gridded data set where grid cell contain the count of 
# Start from UrbanArea_overlay.R


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(lubridate)
library(utils)

#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

#Sudderth drive
codedir <- "~/GitHub/SDI_Waze"  #CONNECT TO VPN FIRST
wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "S:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT"

source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function

setwd(wazedir)

# Read in data
load(file.path(volpewazedir, "Data/MD_hex.RData")) # has the complete grid as hex2, and a subset of just the grid cells in use for this month as hex.md

# Loop through months of available merged data
avail.months = unique(substr(dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))], 
                      start = 17,
                      stop = 18))

temp.outputdir = tempdir()# for temporary storage 

for(j in avail.months){ #j = "05"
  
  load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_MD.RData"))) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
  
  # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
  # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
  link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  
  # <><><><><><><><><><><><><><><><><><><><>
  
  # aggregate: new data frame will have one row per cell, per hour, per day.
  # Response variable column: count of unique EDT events matching Waze events in this cell, within this time window. 
  # Predictor variable columns: median values for the numeric characteristics: report rating, confidence..
  # Counts for the number of waze events of each type and subtype, inside this grid cell at this time.
  # The same, but for each of the neighboring grid cells (N, NE, SE, S, SW, NW). 
  # counts for roadType, 11 columns: length(unique(link.waze.edt$roadType[!is.na(link.waze.edt$roadType)]))
  
  ##############
  
  #Dataframe of Grid IDs by day of year and time of day in month of April data (all grid IDs with Waze or EDT data)
  GridIDall <- c(unique(as.character(link.waze.edt$GRID_ID)), unique(as.character(link.waze.edt$GRID_ID.edt)))
  
  month.days <- unique(as.numeric(format(link.waze.edt$CrashDate_Local, "%d")))
  lastday = max(month.days[!is.na(month.days)])
  
  Month.hour <- seq(from = as.POSIXct(paste0("2017-", j,"-01 0:00"), tz="America/New_York"), 
                    to = as.POSIXct(paste0("2017-", j,"-", lastday, " 23:00"), tz="America/New_York"),
                    by = "hour")
  
  GridIDTime <- expand.grid(Month.hour,GridIDall)
  names(GridIDTime) <- c("GridDayHour","GRID_ID")
  
  
  # Temporally matching
  # Match between the first reported time and last pull time of the Waze event. 
  StartTime <- Sys.time()
  t.min = min(GridIDTime$GridDayHour)
  t.max = max(GridIDTime$GridDayHour)
  i = t.min
  
  Waze.hex.time.all <- vector()
  while(i+3600 <= t.max){
    ti.GridIDTime = filter(GridIDTime,GridDayHour==i)
    ti.link.waze.edt = filter(link.waze.edt, time >= i & time <= i+3600| last.pull.time >=i & last.pull.time <=i+3600)
  
    ti.Waze.hex <- inner_join(ti.GridIDTime, ti.link.waze.edt) #Use left_join to get zeros if no match  
    
    Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex)
    i=i+3600
    print(i)
  } # end loop
  
  EndTime <- Sys.time()-StartTime
  EndTime
  
  Waze.hex.time <- filter(Waze.hex.time.all, !is.na(GRID_ID))
  
  #if the EDT or Waze data have not been updated for a given month, you can skip the Waze.hex.time steps and read in the file directly
  #Split the Waze.hex.time step into a separate script for the cloud pipeline version 
 #uncomment to skip above steps and read in the file directly
    ##load(file.path(paste(outputdir, "/WazeHexTimeList_", j,".RData",sep="")))
  
  
  # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
  
  #summarize counts of Waze events in each hexagon and EDT matches to the Waze events (could be in neighboring hexagon)
  names(Waze.hex.time)
  
  StartTime <- Sys.time()
  waze.hex <- Waze.hex.time %>%
    group_by(GRID_ID, GRID_ID_NW, GRID_ID_N, GRID_ID_NE, GRID_ID_SW, GRID_ID_S, GRID_ID_SE,
             day = format(GridDayHour, "%j"), hour = format(GridDayHour, "%H")) %>%
    summarize(
      DayOfWeek = weekdays(time)[1], #Better way to select one value?
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
      
      nWazeAccidentMajor = n_distinct(uuid.waze[subtype=="ACCIDENT_MAJOR"]),
      nWazeAccidentMinor = n_distinct(uuid.waze[subtype=="ACCIDENT_MINOR"]),
      nWazeHazardCarStoppedRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_CAR_STOPPED"]),
      nWazeHazardCarStoppedShould = n_distinct(uuid.waze[subtype=="HAZARD_ON_SHOULDER_CAR_STOPPED"]),
      nWazeHazardConstruction = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_CONSTRUCTION"]),
      nWazeHazardObjectOnRoad = n_distinct(uuid.waze[subtype=="HAZARD_ON_ROAD_OBJECT"]),
      nWazeJamModerate = n_distinct(uuid.waze[subtype=="JAM_MODERATE_TRAFFIC"]),
      nWazeJamHeavy = n_distinct(uuid.waze[subtype=="JAM_HEAVY_TRAFFIC"]),
      nWazeJamStandStill = n_distinct(uuid.waze[subtype=="JAM_STAND_STILL_TRAFFIC"]),
      nWazeWeatherFlood = n_distinct(uuid.waze[subtype=="HAZARD_WEATHER_FLOOD"]),
      nWazeWeatherFog = n_distinct(uuid.waze[subtype=="HAZARD_WEATHER_FOG"]),
      
      nWazeRT3 = n_distinct(uuid.waze[roadType=="3"]),
      nWazeRT4 = n_distinct(uuid.waze[roadType=="4"]),
      nWazeRT6 = n_distinct(uuid.waze[roadType=="6"]),
      nWazeRT7 = n_distinct(uuid.waze[roadType=="7"]),
      nWazeRT2 = n_distinct(uuid.waze[roadType=="2"]),
      nWazeRT0 = n_distinct(uuid.waze[roadType=="0"]),
      nWazeRT1 = n_distinct(uuid.waze[roadType=="1"]),
      nWazeRT20 = n_distinct(uuid.waze[roadType=="20"])) 
  
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
  
  #test process - look at value for highest count in nWazeAcc_NW column (10)
  t=filter(wazeTime.edt.hex_NW_N_NE_SW_S_SE, GRID_ID=="EG-53" & day=="141" & hour=="15")
  t #10 - this matches, test more
  
  wazeTime.edt.hexAll <- wazeTime.edt.hex_NW_N_NE_SW_S_SE%>%
    mutate_all(funs(replace(., is.na(.), 0)))
  #Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the neighbor grid cell at that hour)
  
  #Update time variable 
  hextimeChar <- paste(wazeTime.edt.hexAll$day,wazeTime.edt.hexAll$hour,sep=":")
  wazeTime.edt.hexAll$hextime <- strptime(hextimeChar, "%j:%H", tz=)

#  save(list="waze.edt.hex", file = paste(temp.outputdir, "/WazeEdtHex_", j,".RData",sep=""))

  #Save list of Grid cells and time windows with EDT or Waze data, so we don't re-run this    
  save(list="Waze.hex.time", file = paste(temp.outputdir, "/WazeHexTimeList_", j,".RData",sep=""))
  write.csv(Waze.hex.time, file = paste(temp.outputdir, "/WazeHexTimeList_", j,".csv",sep=""))

  save(list="wazeTime.edt.hexAll", file = paste(temp.outputdir, "/WazeTimeEdtHexAll_", j,".RData",sep=""))
  write.csv(wazeTime.edt.hexAll, file = paste(temp.outputdir, "/WazeTimeEdtHexAll_", j,".csv",sep=""))
  
} # End month aggregation loop ----

movefiles(dir(temp.outputdir)[grep("Hex", dir(temp.outputdir))], temp.outputdir, outputdir)

##########################################################################################################
##########################################################################################################

#Functions to play with for continued aggregations
  spread(type, uniqWazeEvents) %>%
  add_tally()

df %>% 
  count(a, b) %>%
  slice(which.max(n))

##Sample function to split columns into new rows
cols <- df %>% 
  group_by(ID) %>% 
  filter(!is.na(PHN), !is.na(EMAIL)) %>% 
  group_size() %>% 
  max()

df %>%
  group_by(ID, NAME, ADDRESS) %>%
  summarize_each(funs(toString(unique(.[!is.na(.)]))), EMAIL, PHN) %>% 
  separate_("EMAIL", sprintf("EMAIL%s", 1:cols), sep = ",", fill = "right") %>% 
  separate_("PHN", sprintf("PHN%s", 1:cols), sep = ",", fill = "right") %>% 
  mutate_if(is.character, trimws) %>% 
  mutate_each(funs(replace(., grep("NA", .), NA)))
#

df %>% complete(group, nesting(item_id, item_name), fill = list(value1 = 0))
t <- link.waze.edt[1000:1010,]; t
names(t)
tt <- t %>% complete(uuid.waze, nesting(Waze.start.day, Waze.end.day,Waze.start.hour,Waze.end.hour), fill=list())

summary(!is.na(link.waze.edt$GRID_ID.edt))

help = mutate(help, newvar = ifelse(is.na(var1), as.character(var2), as.character(var1)))

unite_(mtcars, "vs_am", c("vs","am"))
mtcars %>%
  unite(vs_am, vs, am) %>%
  separate(vs_am, c("vs", "am"))

# Extra explortaion:
# how often is the EDT event not in the central grid cell?
summary(link.waze.edt$GRID_ID == link.waze.edt$GRID_ID.edt)

# some Waze events had NA for grid cell ID in the first iteration (fixed now)
map('state','maryland')
points(link.waze.edt[is.na(link.waze.edt$GRID_ID),c("lon","lat")]) 

# EDT events with NA for grid cell ID:
dim(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),])
points(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),c("GPSLong_New","GPSLat")]) # also in NW corner of the state, these should go away with new grid cells from Michelle