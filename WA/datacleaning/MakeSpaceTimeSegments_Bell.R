# Precursor to Grid_aggregation, follows Hex_UA_overlay_SDC


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
rm(list= ls())
library(maps) # for mapping base layers
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(rgeos) # gintersection
library(ggmap)
library(tidyverse)

codeloc <- ifelse(grep('Flynn', normalizePath('~/')),
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze")

source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
output.loc <- file.path(wazeshareddir, "Output/WA")
data.loc <- file.path(wazeshareddir, "Data/Bellevue")

# GIS layer projection
proj <- showP4(showWKT("+init=epsg:6597"))

# Load the network data
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
roadnettb_snapped <- spTransform(roadnettb_snapped, CRS(proj))

# Load Waze data (old data based on 20 months, and the new data is based on 12 months) ----
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Waze_Snapped50ft_MatchName") # 114614*15, new: 70226*17
waze_snapped <- spTransform(waze_snapped, CRS(proj))

# Load unsnapped Waze data as well. time variable was reduced to just date in the shapefile, so we will join with the original data to get precise time
wazetb <- read_csv(file.path(data.loc, 'Export', 'WA_Bellevue_Prep_Export.csv'))
# format timestamp
wazetb$time <- as.POSIXct(wazetb$time, tz = "America/Los_Angeles")
# Getting the timestamp of the Waze events from the original Waze data export
waze_snapped@data <- left_join(waze_snapped@data %>% select(-time),
                               waze_unsnapped %>% select(SDC_uuid, time),
                               by = 'SDC_uuid') 
# Load Crash data ----
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Crashes_Snapped50ft_MatchName") # 2085*29, new: 1369*51 
crash_snapped <- spTransform(crash_snapped, CRS(proj))

# Load unsnapped crash data for confirmation on date
# Raw Crash points - potential to add additional variables to the final model----
crashtb <- read.csv(file = file.path(data.loc, "Crash","20181127_All_roads_Bellevue.csv"))
names(crashtb) # Looks like this is a full crash database
dim(crashtb) # 4417  254, a total of 4417 crashes.

# format timestamp
crashtb$time <- as.POSIXct(paste(crashtb$DATE, crashtb$X24.HR.TIME), tz = 'America/Los_Angeles', format = "%m/%d/%Y %H:%M")
# join the crash snapped data with additional column time
crash_snapped@data <- left_join(crash_snapped@data,
                                crashtb %>% select(REPORT.NUMBER, time),
                                by = c("REPORT_NUM" = "REPORT.NUMBER"))

# Load FARS data -- Not needed, these do not vary by hour so will just use the values in the roadnettb_snapped layer
# Jessie: this will be moved to the aggregation code.
# FARS_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "FARS_Snapped50ft_MatchName") # new: 13*19
# FARS_snapped <- spTransform(FARS_snapped, CRS(proj))

# Load weather data -- Only need to assign to a Date
load(file.path(data.loc, "Weather","Prepared_Bellevue_Wx_2018.RData")) # the file name wx.grd.day
# format day
wx.grd.day$day <- as.Date(wx.grd.day$day)

# <><><><><><><><><><><><><><><><><><><><>
# In Process ----
# Segment unique ID: RDSEG_ID
SegIDall <- roadnettb_snapped$RDSEG_ID

# All year, month, and hour
# Calendar year 2018
# Create date field for Waze from time 
waze_snapped$date <- format(waze_snapped$time, format='%Y-%m-%d')
crash_snapped$DATE <- as.Date(as.character(crash_snapped$DATE))

# Rename OBJECTIDs
colnames(waze_snapped@data)[colnames(waze_snapped@data)=="OBJECTID"] <- "OBJECTID_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="OBJECTID"] <- "OBJECTID_crash"
 
colnames(waze_snapped@data)[colnames(waze_snapped@data)=="HourOfDay"] <- "HourOfDay_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="HourOfDay"] <- "HourOfDay_crash"

colnames(waze_snapped@data)[colnames(waze_snapped@data)=="MinOfDay"] <- "MinOfDay_waze"
colnames(crash_snapped@data)[colnames(crash_snapped@data)=="MinOfDay"] <- "MinOfDay_crash"

# Outerjoining all three datasets.
link.waze.bell <-  waze_snapped@data %>% full_join(crash_snapped@data, by = "RDSEG_ID")

# list of all months
todo.months = format(seq(from = as.Date("2018-01-01"), to = as.Date("2018-12-31"), by = "month"), "%Y-%m")

# Start parallel process
cl <- makeCluster(parallel::detectCores()) 
registerDoParallel(cl)


foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
  # j = todo.months[1]  
  # Format month, day
    year.month.w <- format(link.waze.bell$time, "%Y-%m")
    year.month.t <- format(link.waze.bell$DATE, "%Y-%m")
    year.month <- year.month.w
    year.month[is.na(year.month)] <- year.month.t[is.na(year.month)]
    
    link.waze.bell.j <- link.waze.bell[year.month == j,]
    
    month.days.w  <- unique(as.numeric(format(link.waze.bell.j$time, "%d"))) # Waze event date/time
    month.days.t  <- unique(as.numeric(format(link.waze.bell.j$DATE, "%d"))) # Bellevue crash date/time
    month.days = sort(unique(c(month.days.w[!is.na(month.days.w)], month.days.t[!is.na(month.days.t)])))
    
    lastday = max(month.days[!is.na(month.days)])
    
    Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = 'America/Los_Angeles'), # A list of timezone areas for reference: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
                      to = as.POSIXct(paste0(j,"-", lastday, " 24:00"), tz = 'America/Los_Angeles'),
                      by = "hour")
    
    # Make a segment time all table
    SegIDTime <- expand.grid(Month.hour, SegIDall)
    names(SegIDTime) <- c("SegDayHour", "Seg_ID")
    
    # Assign Waze points to the table
    
    # Assign Bellevue points to the table
    
    cat("Completed", j, "\n") # end the loop
}

stopCluster(cl); gc()



# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# OLD TN CODE FOR REFERENCE ----
# Delete when Bellevue code completed
# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
OLDCODE = F

if(OLDCODE){

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
}