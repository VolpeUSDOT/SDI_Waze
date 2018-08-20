# Identify special events with the Waze data

#### Set up ####
codeloc <- "~/Github/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(foreach)
library(doParallel)
library(rgeos) #gintersection
library(lubridate)
library(ggplot2)

localdir <- "C:/Users/Jessie.Yang.CTR/Downloads/OST/Waze project/Special Events"
# localdir <- "/home/daniel/workingdata/" # full path for readOGR, Jessie don't have this folder.
# edtdir <- normalizePath(file.path(localdir, "EDT"))
# wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary
wazedir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
output.loc <- file.path(wazedir, "WazeEDT Pilot Phase1 Archive Incomplete/SDI/Model_Output")
data.loc <- file.path(wazedir, "WazeEDT Pilot Phase1 Archive Incomplete/SDI/Data")

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


# Project to Albers equal area conic 102008. Check comparision with USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version, used for producing hexagons: 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

#### Read shapefiles and data ####
# Read shapefiles and apply coordinate reference system of hexagons (USGS version of Albers equal area) to Urban Areas and counties using uproj.USGS

# Read in US state shapefile
ua <- readOGR(file.path(data.loc,"Census4Waze"), layer = "cb_2016_us_ua10_500k")
ua <- spTransform(ua, CRS(proj.USGS))

# Read in county shapefile 
co <- readOGR(file.path(data.loc,"Census4Waze"), layer = "cb_2015_us_county_500k")
co <- spTransform(co, CRS(proj.USGS))

md <- readOGR(file.path(data.loc,"Census4Waze"), layer = "MD_outline")
md <- spTransform(md, CRS(proj.USGS))
# md@data # check the data table in SpatialPolygonsDataFrame class

# Read in hexagon shapefile
grid <- readOGR(file.path(data.loc,"MD_hexagons_shapefiles"), layer = "MD_hexagons_1mi_newExtent_neighbors")
grid <- spTransform(grid, CRS(proj.USGS))

# Read in model output and independent variables used in the model
GridCount <- read.csv(paste0(output.loc, "/Waze_04-09_GridCounts.csv")) #623,251*14
AllModel30 <- read.csv(paste0(output.loc, "/All_Model_30.csv")) # 617,338*124

# Convert day of year to date
GridCount$date <- as.Date(GridCount$day, origin = "2016-12-31")
AllModel30$date <- as.Date(AllModel30$day, origin = "2016-12-31")
# as.Date(91, origin = "2016-12-31") # "2017-04-01", the origins need to be the last day of 2016.

length(unique(AllModel30$GRID_ID)) # 5880
length(unique(GridCount$GRID_ID)) # 5176

# Verify the date range
max(GridCount$date) # Sept 30
min(GridCount$date) # April 1
max(AllModel30$date)
min(AllModel30$date) # April 1, 2017

# Read special event data
SpecialEvents <- read.csv(file = paste0(wazedir,"/Data/SpecialEvents/SpecialEvents_MD_AprilToSept_2017.csv"))
SpecialEvents_SP <- SpatialPointsDataFrame(SpecialEvents[c("Lon", "Lat")], SpecialEvents, proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure Waze data is a SPDF
SpecialEvents_SP <-spTransform(SpecialEvents_SP, CRS(proj.USGS)) # create spatial point data frame
plot(SpecialEvents_SP)

#### Special events mini example ####
# Two example events, Baseball game. Fedex Field does not have football event on Sep 17, and M&T Bank Stadium does not have football event on Sept 10. We need to check whether there is any pre-season football events before Sept 10 to have a baseline to compare.
"1:00 PM ETSeptember 10, 2017, FedEx Field, 1600 Fedex Way, Landover, MD 20785
1:00 PM ETSeptember 17, 2017, M&T Bank Stadium, 1101 Russell St, Baltimore, MD 21230"	
"(38.907794, -76.864535) (39.278187, -76.622329)"	
"(FC-64, C-63, FB-64) (FK-38, FK-37, FJ-38)"

# Compare hourly distribution of these pologans, they are mostly zero.
GridCount[GridCount$GRID_ID %in% paste0(1,c("FC-64", "C-63", "FB-64")) & GridCount$date == "2017-09-10",]
# they are all zero, not crashes happened at this day.

AllModel30[AllModel30$GRID_ID %in% paste0(1,c("FC-64", "C-63", "FB-64"))
                    & AllModel30$date == "2017-09-10", c("GRID_ID","hour","nWazeJam","nWazeHazardCarStoppedRoad","nWazeHazardCarStoppedShoulder","nHazardOnRoad")]
# they are all zero, not jams happened at this day.

# # Write special events into a table
# SpecialEvents <- data.frame(location = c("Fedex Field", "Fedex Field"),
#                             event = c("Football@1pm", "No Event"),
#                             date = c("2017-09-10", "2017-09-17"),
#                             day = c(253, 260),
#                             lon = c(-76.864535, -76.864535),
#                             lat = c(38.907794,38.907794),
#                             buffer = 3) # FedEx Field
# write.csv(SpecialEvents, paste0(localdir, "/SpecielEvents.csv"), row.names = F)
# 
# SpecialEvents_SP <- SpatialPointsDataFrame(SpecialEvents[c("Lon", "Lat")], SpecialEvents, proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure Waze data is a SPDF
# SpecialEvents_SP <-spTransform(SpecialEvents_SP, CRS(proj.USGS)) # create spatial point data frame
# plot(SpecialEvents_SP)
# 
# buffdist <- SpecialEvents$buffer*1609 # convert miles to meters
# SpecialEvents_buffer <- gBuffer(SpecialEvents_SP, width = buffdist[1]) # create a buffer, look for buffer_state.R for more information on spatial join

#### Special Event Data Process ####
plot(SpecialEvents_buffer) # plot the spatial

grid@data # look at data from the grid 
# over(SpecialEvents_buffer, grid, fn = mean) # join polygons to SpatialPolygonDataFrame, return to only one row of mean values in the data frame.
gIntersects(SpecialEvents_buffer, grid, byid = T) # a data frame with T/F logic values of the grid data frame
sum(gIntersects(SpecialEvents_buffer, grid, byid = T)) # 42 polygons are intersected

grid_id <- grid$GRID_ID[gIntersects(SpecialEvents_buffer, grid, byid = T)]
grid_id <- paste0(1,grid_id)

grid_overlap <- expand.grid(GRID_ID = grid_id, hour = c(0:23), day = c(253, 260))

# SpecialEvents <- SpecialEvents %>% left_join(grid@data)

# GridCount[GridCount$GRID_ID %in% grid_id & GridCount$date == "2017-09-10",]

# # fill in the GridCount for all hours
# blank.grid <- expand(GridCount, GRID_ID, day, hour)
# blank.grid

# Columns with numeric counts or values
col.names <- names(GridCount)[-c(1:4,15)]

# GridCount_new <- blank.grid %>% left_join(GridCount, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31")) # 30256488 = 24*183*6889

# dim(GridCount_new) #30256488       15
length(unique(GridCount$day)) #183
length(unique(GridCount$date)) #183
length(unique(GridCount$GRID_ID)) #6889

# GridCount_sum <- 

# t-test accident counts between event and a week after the event
y <- grid_overlap %>% left_join(GridCount, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31"), DayOfWeek = as.integer(factor(weekdays(date),levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))) %>% left_join(SpecialEvents, by =  c("day") )

y1 <- y %>% filter(day == 253)

y2 <- y %>% filter(day == 260)

# T-test on accidents between two days.
t.test(y1$nWazeAccident,y2$nWazeAccident,paired=TRUE) # if buffer = 2 mile, p-value = 0.7154, not significant; buffer = 3 mile, p-value=0.035, significant

# remove all other objects, and run the script again
# rm(list=setdiff(ls(), "AllModel30"))

# Time series plot
y_sum <- y %>% group_by(event, hour) %>% summarize(nWazeAccident = mean(nWazeAccident))
ggplot(y_sum, aes( x = hour, y = nWazeAccident)) + geom_point() + geom_line() + facet_wrap(~ event) + ylab("Average Waze Accident")
ggsave(paste0(wazedir,"/Output/visualizations/Special_event_example1.png"))

# Other variables
col.names <- c("nWazeJam")

# t-test accident counts between event and a week after the event
y <- grid_overlap %>% left_join(AllModel30, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31"), DayOfWeek = as.integer(factor(weekdays(date),levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))) %>% left_join(SpecialEvents, by =  c("day") )

y1 <- y %>% filter(day == 253)

y2 <- y %>% filter(day == 260)

# T-test on accidents between two days.
t.test(y1$nWazeJam,y2$nWazeJam,paired=TRUE) #  buffer = 3 mile, p-value=0.2326

# remove all other objects, and run the script again
# rm(list=setdiff(ls(), "AllModel30"))

# Time series plot
y_sum <- y %>% group_by(event, hour) %>% summarize(nWazeJam = mean(nWazeJam))
ggplot(y_sum, aes( x = hour, y = nWazeJam)) + geom_point() + geom_line() + facet_wrap(~ event) + ylab("Average Waze Jam")
ggsave(paste0(wazedir,"/Output/visualizations/Special_event_example1_Jam.png"))


