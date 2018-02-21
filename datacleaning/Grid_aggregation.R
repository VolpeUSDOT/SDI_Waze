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
outputdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function

setwd(wazedir)

# Read in data

load(file.path(wazemonthdir, "merged.waze.edt.April_MD.RData")) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
load(file.path(volpewazedir, "spatial_layers/MD_hex.RData")) # has the complete grid as hex2, and a subset of just the grid cells in use for this month as hex.md

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

# EAS test - aggregate all to grid cell, then add data for neighboring grid cells using mutate
# 27,878 grid cells
names(link.waze.edt)

#summarize counts of Waze events in each hexagon and EDT matches to the Waze events (could be in neighboring hexagon)
#TODO: Expand Waze events over time windows (time to last.pull.time) - right now, they only show up in the start time windows(?) - only matters for persistent Waze events.

waze.edt.hex <- 
link.waze.edt %>%
  group_by(GRID_ID, day = format(time, "%j"), hour = format(time, "%H")) %>%
  summarize(
    DayOfWeek = weekdays(time)[1], #Better way to select one value?
    nRows = n(), #includes duplicates that match more than one EDT report (don't use in model)
    uniqWazeEvents= n_distinct(uuid.waze),
    nMatchEDT_buffer = n_distinct(CrashCaseID[which(match=="M")]),
    nMatchWaze_buffer = n_distinct(uuid.waze[which(match=="M")]),
    nNoMatchWaze_buffer = n_distinct(uuid.waze[which(match=="W")]),
    nWazeAccident = n_distinct(uuid.waze[which(subtype=="ACCIDENT")]),
    nWazeJam = n_distinct(uuid.waze[which(subtype=="JAM")]),
    nWazeRoadCloased = n_distinct(uuid.waze[which(subtype=="ROAD_CLOSED")]),
    nWazeWeatherOrHazard = n_distinct(uuid.waze[which(subtype=="WEATHERHAZARD")]),
    nWazeAccidentMajor = n_distinct(uuid.waze[which(subtype=="ACCIDENT_MAJOR")]),
    nWazeAccidentMinor = n_distinct(uuid.waze[which(subtype=="ACCIDENT_MINOR")]),
    nWazeHazardCarStoppedRoad = n_distinct(uuid.waze[which(subtype=="HAZARD_ON_ROAD_CAR_STOPPED")]),
    nWazeHazardCarStoppedShould = n_distinct(uuid.waze[which(subtype=="HAZARD_ON_SHOULDER_CAR_STOPPED")]),
    nWazeHazardConstruction = n_distinct(uuid.waze[which(subtype=="HAZARD_ON_ROAD_CONSTRUCTION")]),
    nWazeHazardObjectOnRoad = n_distinct(uuid.waze[which(subtype=="HAZARD_ON_ROAD_OBJECT")]),
    nWazeJamModerate = n_distinct(uuid.waze[which(subtype=="JAM_MODERATE_TRAFFIC")]),
    nWazeJamHeavy = n_distinct(uuid.waze[which(subtype=="JAM_HEAVY_TRAFFIC")]),
    nWazeJamStandStill = n_distinct(uuid.waze[which(subtype=="JAM_STAND_STILL_TRAFFIC")]),
    nWazeWeatherFlood = n_distinct(uuid.waze[which(subtype=="HAZARD_WEATHER_FLOOD")]),
    nWazeWeatherFog = n_distinct(uuid.waze[which(subtype=="HAZARD_WEATHER_FOG")])) 

#??Is there a faster way than "which" to get these values?

save(list="waze.edt.hex", file = "WazeEdtHex_Beta.RData")

#TODO: Complete similar process for EDT M and EDT only reports, then merge     

##########################################################################################################
#Functions to play with for continued aggregations
  spread(type, uniqWazeEvents) %>%
  add_tally()


#Vector of Grid IDs in month of data (all grid IDs with Waze or EDT data)
GridIDall <- c(unique(as.character(link.waze.edt$GRID_ID)), unique(as.character(link.waze.edt$GRID_ID.edt)))
Aprilday <- seq(ymd('2017-04-01'),ymd('2017-04-30'), by = '1 day')
day <- format(Aprilday, "%j")
hour <- seq(1,24,1)
GridIDTime <- expand.grid(hour,day,GridIDall)



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

# some Waze events have NA for grid cell ID, which should not be the case
map('state','maryland')
points(link.waze.edt[is.na(link.waze.edt$GRID_ID),c("lon","lat")]) 

# EDT events with NA for grid cell ID:
dim(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),])
points(link.waze.edt[is.na(link.waze.edt$GRID_ID.edt) & !is.na(link.waze.edt$CrashCaseID),c("GPSLong_New","GPSLat")]) # also in NW corner of the state, these should go away with new grid cells from Michelle

