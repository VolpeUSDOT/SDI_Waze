# Takes the shape files prepared in ArcGIS from the Bellevue point data and road network.
# Inputs: 1) RoadNetwork_Jurisdiction_withData.shp; 2) Crashes_Snapped50ft_MatchName.shp; 3) "Waze_Snapped50ft_MatchName.shp
#         4) WA_Bellevue_Prep_Export.csv; 5) 20190314_All_roads_Bellevue.csv
# Outputs: twelve WazeSegTimeList_2018-01.RData for calendar year 2018. 
# These are lists of segment and time windows with crash or Waze data for each hour. 
# Precursor to Segment_Aggregation_Bell.R
# Code validated on 4/1/2019


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
rm(list= ls())
library(sp)
library(rgdal) # for readOGR(),writeOGR() needed for reading/writing in ArcM shapefiles
library(tidyverse)
library(doParallel)

codeloc <- "~/WA/code" #replace with file path for code folder

source(file.path(codeloc, 'utility/get_packages.R'))

## Working directories
wazeshareddir <- "~/WA" #replace with path to root directory
data.loc <- "~/WA/data" #replace with path to data folder in root directory

# GIS layer projection
proj <- showP4(showWKT("+init=epsg:6597"))

# Load the network data, for the definitive RDSEG_ID to join to
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData")
roadnettb_snapped <- spTransform(roadnettb_snapped, CRS(proj))

# Load Waze data (old data based on 20 months, and the new data is based on 12 months) ----
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Waze_Snapped50ft_MatchName")
waze_snapped <- spTransform(waze_snapped, CRS(proj))

# Load unsnapped Waze data to get precise time
# To get event duration: Export again with last.pull.time
wazetb <- read_csv(file.path(data.loc, 'Export', 'WA_Bellevue_Prep_Export.csv'),
                   locale = locale(tz = 'America/Los_Angeles'))

# Getting the timestamp of the Waze events from the original Waze data export
waze_snapped@data <- left_join(waze_snapped@data %>% select(-time),
                               wazetb %>% select(SDC_uuid, time),
                               by = 'SDC_uuid') 
stopifnot(sum(is.na(waze_snapped$time)) == 0) # check if there are any NAs.
stopifnot(any(class(waze_snapped$time) %in% 'POSIXct')) # time is correctly formatted.

# Load Crash data ----
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Crashes_Snapped50ft_MatchName")
crash_snapped <- spTransform(crash_snapped, CRS(proj))

# Load unsnapped crash data for confirmation on date
# Raw Crash points - potential to add additional variables to the final model
crashtb <- read.csv(file = file.path(data.loc, "Crash","20190314_All_roads_Bellevue.csv"))

# format timestamp
crashtb$time <- as.POSIXct(paste(crashtb$DATE, crashtb$X24.HR.TIME), tz = 'America/Los_Angeles', format = "%m/%d/%Y %H:%M")
# join the crash snapped data with additional column time
crash_snapped@data <- left_join(crash_snapped@data,
                                crashtb %>% select(REPORT.NUMBER, time),
                                by = c("REPORT_NUM" = "REPORT.NUMBER"))
stopifnot(sum(is.na(crash_snapped$time)) == 0) # check if there are any NAs.
stopifnot(any(class(crash_snapped$time) %in% 'POSIXct')) # time is correctly formatted.


# <><><><><><><><><><><><><><><><><><><><>
# Process to monthly segment x hour files ----
# Segment unique ID: RDSEG_ID
SegIDall <- roadnettb_snapped$RDSEG_ID

# All year, month, and hour
# Calendar year 2018
# list of all months to do. 
todo.months = format(seq(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "month"), "%Y-%m")

# Format month, day. Use these for indexing within the loop. Keep track of .w and .c
year.month.w <- format(waze_snapped$time, "%Y-%m")
year.month.c <- format(crash_snapped$time, "%Y-%m")

# Start parallel process
cl <- makeCluster(parallel::detectCores()) 
registerDoParallel(cl)

# Store messages in a log file
writeLines(c(""), "Make_SpaceTime_log.txt")    

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
  sink("Make_SpaceTime_log.txt", append=TRUE)
  cat(paste(Sys.time()), j, "\n") 
  waze.j <- waze_snapped[year.month.w == j,]
  crash.j <- crash_snapped[year.month.c == j,]

  month.days.w  <- unique(as.numeric(format(waze.j$time, "%d"))) # Waze event date/time
  month.days.c  <- unique(as.numeric(format(waze.j$time, "%d"))) # Bellevue crash date/time
  month.days = sort(unique(c(month.days.w[!is.na(month.days.w)], month.days.c[!is.na(month.days.c)])))
  
  lastday = max(month.days[!is.na(month.days)])
 
   # A list of timezone areas for reference: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = 'America/Los_Angeles'), 
                    to = as.POSIXct(paste0(j,"-", lastday, " 24:00"), tz = 'America/Los_Angeles'),
                    by = "hour") # create a sequential list of timestamps including date and time (for every hour).
  
  # Make a segment time all table
  SegIDTime <- expand.grid(Month.hour, SegIDall)
  names(SegIDTime) <- c("SegDayHour", "RDSEG_ID")
  
  StartTime <- Sys.time()
  t.min = min(SegIDTime$SegDayHour) 
  t.max = max(SegIDTime$SegDayHour) 
  i = t.min
  
  Waze.seg.time.all <- vector()
  Crash.seg.time.all <- vector()
  
  counter = 1
  while(i+3600 <= t.max){
    ti.SegIDTime = filter(SegIDTime, SegDayHour == i)
    # Match Waze events by time time. Use last pull time if available (in this dataset, last pull time is not avialable)
    if('last.pull.time' %in% names(waze.j)){
      ti.link.waze = waze.j@data %>% filter(time >= i & time <= i+3600 | last.pull.time >= i & last.pull.time <=i+3600) 
    } else {
      ti.link.waze = waze.j@data %>% filter(time >= i & time <= i+3600)  
      }
    
    # Match crash events by time
    ti.link.crash = crash.j@data %>% filter(time >= i & time <= i+3600) 
    
    ti.Waze.seg.w <- inner_join(ti.SegIDTime, ti.link.waze, by = "RDSEG_ID") 
    ti.Waze.seg.c <- inner_join(ti.SegIDTime, ti.link.crash, by = "RDSEG_ID")
    
    Waze.seg.time.all <- rbind(Waze.seg.time.all, ti.Waze.seg.w)
    Crash.seg.time.all <- rbind(Crash.seg.time.all, ti.Waze.seg.c)
    
    i=i+3600
    counter = counter + 1
    if(counter %% 3600*24 == 0) cat(paste(i, "\n")) # If counter == 0, will output the time with issues.
  } # end loop
  
  # Remove any duplicates and NA segment IDs. Should be none.
  Waze.seg.time <- unique(Waze.seg.time.all) 
  Waze.seg.time <- filter(Waze.seg.time, !is.na(RDSEG_ID))

  Crash.seg.time <- unique(Crash.seg.time.all) 
  Crash.seg.time <- filter(Crash.seg.time, !is.na(RDSEG_ID))
  
  #Save list of segment and time windows with crash or Waze data  
  fn = paste0("WazeSegTimeList_", j, ".RData")
  
  save(list=c("Waze.seg.time", "Crash.seg.time"), file = file.path(data.loc, 'Segments', fn))
  
  timediff = Sys.time() - StartTime
  
  cat("Completed", j, 'in', round(timediff, 2), attr(timediff, 'units'), "\n") # end the loop
}

stopCluster(cl); gc()

