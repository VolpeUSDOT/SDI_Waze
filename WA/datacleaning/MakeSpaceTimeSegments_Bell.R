# Precursor to Grid_aggregation, follows Hex_UA_overlay_SDC


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
rm(list= ls())
library(maps) # for mapping base layers
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(rgeos) # gintersection
library(ggmap)
library(spatstat) # for ppp density estimation. devtools::install_github('spatstat/spatstat')
library(tidyverse)

codeloc <- "~/GitHub/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
output.loc <- file.path(wazeshareddir, "Output/WA")
data.loc <- file.path(wazeshareddir, "Data/Bellevue")

# GIS layer projection
proj <- showP4(showWKT("+init=epsg:6597"))

# Load the network data
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "RoadNetwork_Jurisdiction_withIntersections_FullCrash") # 6647 * 14
roadnettb_snapped <- spTransform(roadnettb_snapped, CRS(proj))

# Load Waze data
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "WazeReports_Snapped50ft_MatchName")
waze_snapped <- spTransform(waze_snapped, CRS(proj))

# Load Crash data
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "CrashReports_Snapped50ft_MatchName")
crash_snapped <- spTransform(crash_snapped, CRS(proj))

# <><><><><><><><><><><><><><><><><><><><>
# ----
# TO DO:
#   1. Need the timestamp of the Waze events, Dan exported the updated Waze events, will need Michelle to pre-process that.

# For now, using the objectID as the unique ID of a segment, renames it as segment ID
SegIDall <- roadnettb_snapped$OBJECTID

# All year, month, and hour
# Calendar year 2018
# Convert Waze time with time stamp
waze_snapped$time <- as.Date(waze_snapped$time, format='%Y/%M/%d')
crash_snapped$DATE <- as.Date(crash_snapped$DATE, format = '%Y/%M/%d')

# Rename OBJECTIDs
colnames(waze_snapped@data)[colnames(waze_snapped@data)=="OBJECTID"] <- "OBJECTID_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="OBJECTID"] <- "OBJECTID_crash"
 
colnames(waze_snapped@data)[colnames(waze_snapped@data)=="HourOfDay"] <- "HourOfDay_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="HourOfDay"] <- "HourOfDay_crash"

colnames(waze_snapped@data)[colnames(waze_snapped@data)=="MinOfDay"] <- "MinOfDay_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="MinOfDay"] <- "MinOfDay_crash"

# Outerjoining all three datasets.
link.waze.bell <-  waze_snapped@data %>% full_join(crash_snapped@data, by = "NEAR_FID")

# list of all months
todo.months = format(seq(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "month"), "%Y-%m")

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
    # Format month, day
    year.month.w <- format(link.waze.bell$time, "%Y-%m")
    year.month.t <- format(link.waze.bell$DATE, "%Y-%m")
    year.month <- year.month.w
    year.month[is.na(year.month)] <- year.month.t[is.na(year.month)]
    
    link.waze.bell <- link.waze.bell[year.month == j,]
    
    month.days.w  <- unique(as.numeric(format(link.waze.bell$time, "%d"))) # Waze event date/time
    month.days.t  <- unique(as.numeric(format(link.waze.bell$DATE, "%d"))) # TN crash date/time
    month.days = unique(c(month.days.w, month.days.t))
    
    lastday = max(month.days[!is.na(month.days)])
    
    Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = 'Pacific/Auckland'), 
                      to = as.POSIXct(paste0(j,"-", lastday, " 24:00"), tz = 'Pacific/Auckland'),
                      by = "hour")
    
    # Make a segment time all table
    SegIDTime <- expand.grid(Month.hour, SegIDall)
    names(SegIDTime) <- c("SegDayHour", "Seg_ID")
    
    # Assign Waze points to the table
    
    # Assign Bellevue points to the table
    
    cat("Completed", j, "\n") # end the loop
}


# start segment loop ----
    ##############
    # Make data frame of all Grid IDs by day of year and time of day in each month of data (subset to all grid IDs with Waze OR EDT data)
    GridIDall <- unique(c(as.character(link.waze.tn$GRID_ID), # Grid ID for a Waze event
                          as.character(link.waze.tn$GRID_ID.TN)) # Grid ID for a TN crash
                        )
    # x <- data.frame(length(unique(as.character(link.waze.tn$GRID_ID))), length(unique(as.character(link.waze.tn$GRID_ID.TN))), length(GridIDall), length(unique(GridIDall)))
    # 837, 698, 1535, 922 # using "2017-04" as an example, apparently, we have some duplication in GridIDall
    
    # Date/time for TN crash only events or TN/Waze matching events are stored as 'date', while Date/time for Waze only events are stored as 'time'.
    year.month.w <- format(link.waze.tn$time, "%Y-%m")
    year.month.t <- format(link.waze.tn$date, "%Y-%m")
    year.month <- year.month.w
    year.month[is.na(year.month)] <- year.month.t[is.na(year.month)]
    
    link.waze.tn <- link.waze.tn[year.month == j,]
    
    month.days.w  <- unique(as.numeric(format(link.waze.tn$time, "%d"))) # Waze event date/time
    month.days.t  <- unique(as.numeric(format(link.waze.tn$date, "%d"))) # TN crash date/time
    month.days = unique(c(month.days.w, month.days.t))
    
    lastday = max(month.days[!is.na(month.days)])
    
    Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = 'America/Chicago'), 
                      to = as.POSIXct(paste0(j,"-", lastday, " 24:00"), tz = 'America/Chicago'), # Why timezone of from and to are different? Looks like the code only used CDT
                      by = "hour")
    
    GridIDTime <- expand.grid(Month.hour, GridIDall)
    names(GridIDTime) <- c("GridDayHour", "GRID_ID")
  
    # Temporally matching
    # Match between the first reported time and last pull time of the Waze event. 
    StartTime <- Sys.time()
    t.min = min(Month.hour) # format(min(Month.hour), "%Y-%m-%d %H:%M:%S %Z") # the datetime format of the first hour showed as date only format. When reformat using format() function, it did not work.
    t.max = max(Month.hour) # format(max(Month.hour), "%Y-%m-%d %H:%M:%S %Z")
    i = t.min
    
    Waze.hex.time.all <- vector()
    counter = 1
    while(i+3600 <= t.max){
      ti.GridIDTime = filter(GridIDTime, GridDayHour == i)
      ti.link.waze.tn = link.waze.tn %>% filter(time >= i & time <= i+3600 | last.pull.time >= i & last.pull.time <=i+3600) # Match Waze events time
      # table(format(link.waze.tn$time, "%Z")) # all Waze events are in EDT timezone.
      ti.link.waze.tn.t = link.waze.tn %>% filter(date >= i & date <= i+3600) # Match TN crash time
      # table(format(link.waze.tn$date, "%Z")) # All TN crashes are in CDT timezone. One solution is to format the original dataset by setting the local time with accurate timezone information. Any events fall in one Eastern timezone will have "EDT" in their timestamp.
      
      ti.Waze.hex <- inner_join(ti.GridIDTime, ti.link.waze.tn, by = "GRID_ID") # Use left_join to get zeros if no match  
      ti.Waze.hex.t <- inner_join(ti.GridIDTime, ti.link.waze.tn.t, by = "GRID_ID") # Same, for TN only crashes
      
      Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex)
      Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex.t)
      
      i=i+3600
      if(counter %% 3600*24 == 0) cat(paste(i, "\n"))
     } # end loop
    
    EndTime <- Sys.time() - StartTime
    cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
    
    Waze.hex.time <- unique(Waze.hex.time.all) # Rows with match = "M" are duplicated, so we want to remove the duplicates.
    Waze.hex.time <- filter(Waze.hex.time, !is.na(GRID_ID))
    
    #Save list of Grid cells and time windows with EDT or Waze data  
    fn = paste0("WazeHexTimeList_", j,"_", g, "TN.RData")
    
    save(list="Waze.hex.time", file = file.path(temp.outputdir, fn))
    
    cat("Completed", j, "\n")
    } # End SpaceTimeGrid loop ----
  
  stopCluster(cl); gc()
  
} # end grid loop
