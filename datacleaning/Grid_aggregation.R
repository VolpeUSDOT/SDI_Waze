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

#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

#Sudderth drive
# wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
# wazedir <- "S:/SDI Pilot Projects/Waze/"
# volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
# outputdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

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

# 'Manual' way to make the counts in neighboring cells with a loop and matching
# For each neighbor cell, count the number of Waze events of each type and subtype. End result is a data frame with (number of types) x 6 + (number of substypes) x 6 columns, associated with each Waze event.
neighbor.counts.waze <- vector()





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
    nrecord = n()
  )

# Gridded data frame of EDT data
grd.e <- edt.df %>%
  group_by(GRID_ID.edt, day = format(CrashDate_Local, "%j"), hour = format(CrashDate_Local, "%H")) %>%
  summarise(
    nrecord = n(),
    n.N = sum(GRID_ID_N.edt)
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

