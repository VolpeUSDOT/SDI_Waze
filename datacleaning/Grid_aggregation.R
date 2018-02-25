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
load(file.path(volpewazedir, "spatial_layers/MD_hex.RData")) # has the complete grid as hex2, and a subset of just the grid cells in use for this month as hex.md

# Loop through months of available merged data
avail.months = unique(substr(dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))], 
                      start = 17,
                      stop = 18))

temp.outputdir = tempdir()# for temporary storage 

for(j in avail.months){ # j = "05"
  
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
  
  Waze.hex.time <- vector()
  while(i+3600 <= t.max){
    ti.GridIDTime = filter(GridIDTime,GridDayHour==i)
    ti.link.waze.edt = filter(link.waze.edt, time >= i & time <= i+3600| last.pull.time >=i & last.pull.time <=i+3600)
  
    ti.Waze.hex <- inner_join(ti.GridIDTime, ti.link.waze.edt) #Use left_join to get zeros if no match  
    
    Waze.hex.time <- rbind(Waze.hex.time, ti.Waze.hex)
    i=i+3600
    print(i)
  } # end loop
  
  EndTime <- Sys.time()-StartTime
  EndTime
  
  
  #summarize counts of Waze events in each hexagon and EDT matches to the Waze events (could be in neighboring hexagon)
  
  names(Waze.hex.time)
  StartTime <- Sys.time()
  
  waze.hex <- filter(Waze.hex.time, !is.na(GRID_ID)) %>%
    group_by(GRID_ID, day = format(GridDayHour, "%j"), hour = format(GridDayHour, "%H")) %>%
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
      nWazeRoadCloased = n_distinct(uuid.waze[type=="ROAD_CLOSED"]),
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
  names(edt.df)
  edt.hex <- 
    edt.df %>%
    group_by(GRID_ID.edt, day = format(CrashDate_Local, "%j"), hour = format(CrashDate_Local, "%H")) %>%
    summarize(
      uniqEDTreports= n_distinct(CrashCaseID)) 
  
  #Merge EDT counts to waze counts by hexagons
  names(waze.hex)
  names(edt.hex)
  colnames(edt.hex)[1] <- "GRID_ID"
  
  wazeTime.edt.hex <- full_join(waze.hex, edt.hex, by = c("GRID_ID", "day", "hour")) %>%
    mutate_all(funs(replace(., is.na(.), 0)))
  #Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or EDT crashes in the grid cell at that hour)
  
 # names(waze.edt.hex)
  
#  save(list="waze.edt.hex", file = paste(temp.outputdir, "/WazeEdtHex_", j,".RData",sep=""))
  save(list="wazeTime.edt.hex", file = paste(temp.outputdir, "/WazeTimeEdtHex_", j,".RData",sep=""))
  write.csv(wazeTime.edt.hex, file = paste(temp.outputdir, "/WazeTimeEdtHex_", j,".csv",sep=""))

} # End month aggregation loop ----

movefiles(dir(temp.outputdir)[grep("Hex", dir(temp.outputdir))], temp.outputdir, outputdir)

##########################################################################################################
#Not sure we need this now...
#Add time variables
StartTime <- Sys.time()
link.waze.edt <- link.waze.edt %>%
  mutate(Waze.start.year = format(time, "%Y"), 
         Waze.start.day = format(time, "%j"), 
         Waze.start.DofW = format(time, "%A"), 
         Waze.start.hour = format(time, "%H"), 
         Waze.end.year = format(last.pull.time, "%Y"),
         Waze.end.day = format(last.pull.time, "%j"),
         Waze.end.hour = format(last.pull.time, "%H"),
         Waze.end.DofW = format(last.pull.time, "%A"),
         Waze.Duration.Minutes = round((last.pull.time-time)/60,0))%>%
  mutate(Waze.start.TimeID=as.numeric(Waze.start.day)*as.numeric(Waze.start.hour), Waze.end.TimeID=as.numeric(Waze.end.day)*as.numeric(Waze.end.hour))
#Note: this does not work for the zeros!
EndTime <- Sys.time()-StartTime
EndTime


#Functions to play with for continued aggregations
  spread(type, uniqWazeEvents) %>%
  add_tally()

df %>% 
  count(a, b) %>%
  slice(which.max(n))

df %>%
  group_by(hour) %>%
  filter(!hour%%2 & row_number() <3)

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


# Temporally matching
# Match between the first reported time and last pull time of the Waze event. Last pull time is after the earliest time of EDT, and first reported time is earlier than the latest time of EDT
d.t <- d.sp[d.sp[,inctimevar2] >= ei[,acctimevar]-60*60 & d.sp[,inctimevar1] <= ei[,acctimevar]+60*60,] 

id.accident <- rep(as.character(ei[,accidvar]), nrow(d.t))
id.incidents <- as.character(d.t[,incidvar])




link.waze.edt <- mutate(link.waze.edt, GridIDall = ifelse(is.na(GRID_ID), as.character(GRID_ID.edt), as.character(GRID_ID)))

summary(!is.na(link.waze.edt$GRID_ID.edt))
help = mutate(help, newvar = ifelse(is.na(var1), as.character(var2), as.character(var1)))

unite_(mtcars, "vs_am", c("vs","am"))
mtcars %>%
  unite(vs_am, vs, am) %>%
  separate(vs_am, c("vs", "am"))


## Not used right now
# 'Manual' way to make the counts in neighboring cells with a loop and matching
# For each neighbor cell, count the number of Waze events of each type and subtype. 
#End result is a data frame with (number of types) x 6 + (number of substypes) x 6 columns, associated with each Waze event.

neighbor.counts.waze <- vector()

for(i in 1:nrow(link.waze.edt)){
  i = 1
  wx <- link.waze.edt[i,]
  
  link.waze.edt %>%
    filter(GRID_ID == as.character(wx$GRID_ID_N)) %>%
    group_by(day = format(time, "%j"), hour = format(time, "%H"), type, subtype) %>%
    summarise(
      nrecord=n()
    )
  
  }

# Do this by hour now. Then consider aggregation in 3hr blocks, with lag/lead 1hr (maybe) for every Waze events 
# Additiona data: weather, roadway


# making gridded data frame of Waze data, by grid cell ID, day of year, and hour of day.
grd.w <- link.waze.edt %>%
  group_by(GRID_ID, day = format(time, "%j"), hour = format(time, "%H")) %>%
  summarise(
    
    # median.reportRating = median(reportRating),
    # median.confidence = median(confidence),
    # median.reliability = median(reliability),
    # pubMillis = min(pubMillis),
    # first.file = sort(filename, decreasing = F)[1],
    # last.file = sort(filename, decreasing = T)[1],
    nrecord = n(),
    nmatch = table(match)[2] # creating a table every time... slow-ish
  )

# Gridded data frame of EDT data
grd.e <- edt.df %>%
  group_by(GRID_ID.edt, day = format(CrashDate_Local, "%j"), hour = format(CrashDate_Local, "%H")) %>%
  summarise(
    nrecord = n()#,
   # n.N = sum(GRID_ID_N.edt)
  )


# Extra explortaion:
# how often is the EDT event not in the central grid cell?
summary(link.waze.edt$GRID_ID == link.waze.edt$GRID_ID.edt)

# some Waze events had NA for grid cell ID in the first iteration (fixed now)
map('state','maryland')
points(link.waze.edt[is.na(link.waze.edt$GRID_ID),c("lon","lat")]) 

# EDT events with NA for grid cell ID:
dim(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),])
points(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),c("GPSLong_New","GPSLat")]) # also in NW corner of the state, these should go away with new grid cells from Michelle

