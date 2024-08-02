# Background ----------------------------

library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)
library(tigris)
library(doParallel)
library(lubridate)

rm(list=ls()) # clear enviroment

outputdir <- file.path(getwd(),"Output") # to hold daily output files as they are generated
inputdir <- file.path(getwd(),"Input")

# Identify state for analysis; Need to specify 'Washington State' for Washington
state <- "WA" 

state_osm <- ifelse(state == "WA", 'Washington State',
                    ifelse(state == "MN", "Minnesota", NA))

state_bbox <- getbb(state_osm) # Retrieves relevant coordinates; always a rectangle

state_osm <- gsub(" ", "_", state_osm) # Normalize state name

# Projection 
projection <- 5070 

# Year
year <- 2021

# Switching to operate month-by-month to avoid running out of memory 
# when trying to work with an entire year all at the same time.
monthIDs <- formatC(1:12, width = 2, flag = "0")
yearmonths <- c(paste(year, monthIDs, sep="-"))
yearmonths.1 <- paste(yearmonths, "01", sep="-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

month_frames = list()
for (m in 1:12){
  num_days = lastdays[m]
  month = rep(monthIDs[m],times = 24 * num_days)
  day = seq(from=1, to=num_days, by=1)
  day = formatC(day, width = 2, flag = "0")
  day = rep(day, each=24)
  hour = rep(0:23,times=num_days)
  hour = formatC(hour, width = 2, flag = "0")
  temp <- cbind(month,day,hour)
  month_frames[[m]] <- temp
}
gc()

# yearmonths.1 <- paste(yearmonths.1,"00:00:00", sep=" ")
# yearmonths.end <- paste(yearmonths.end, "23:59:59", sep=" ")

# Load Road Data ----------------------

##select server to query OSM api
#If the below doesn't work, try https://overpass.kumi.systems/api/interpreter

new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)


##identify road types you'd like to query for; can pick from c('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential')
road_types <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary')

#initialize object
roadways <- list()

network_file <- paste0(state_osm,"_network")
boundary_file <- paste0(state_osm,"_boundary")
file_path <- file.path(inputdir,'Roads_Boundary', state_osm, paste0(network_file, '.gpkg'), paste0(network_file,'.shp'))

if (file.exists(file.path(file_path))){
  state_network <- read_sf(file_path)
  print("File Found")
} else{

  n = as.numeric(length(road_types)) 
  datalist <- list()
  datalist = vector("list", length = n)
  
  l = 0
  
  for (i in road_types){
    l = l + 1 
    print(paste("File Not Found. Pulling", state, i, "OSM Data from Server"))
    map_data <- opq(bbox = state_bbox) %>%
      add_osm_feature(key = 'highway', value = i) %>%
      osmdata_sf() 
    lines <- map_data$osm_lines
    
    datalist[[l]] <- lines
    
  }
  
  total_network <- do.call(bind_rows, datalist)

  # if (!(dir.exists(state_osm))){
  #   dir.create(state_osm)}
  
  total_network <- st_transform(total_network, crs = projection) 
  
# Pull state boundaries
  if (state_osm == 'Washington_State'){
    state_border <- 'Washington'
  }else{
    state_border <- state_osm
  }
state_maps <- states(cb = TRUE, year = 2021) %>%
  filter_state(state_border) %>%
  st_transform(crs = projection)

#Filter out roadways outside the state

state_network <- st_join(total_network, state_maps, join = st_within) %>%
  filter(!is.na(NAME)) %>%
  select(osm_id, highway, ref, geometry)

write_sf(state_network, file.path(inputdir,'Roads_Boundary', state_osm, paste0(network_file, '.gpkg')), driver = "ESRI Shapefile")
write_sf(state_border, file.path(inputdir,'Roads_Boundary', state_osm, paste0(boundary_file, '.gpkg')), driver = "ESRI Shapefile")
rm(state_border)
}


#ggplot() + geom_sf(data = state_network)

#Transform state_network crs to NAD83 before joining with crash_files; should probably change this in the files we have and bring this into the query loop

if(st_crs(state_network) != projection){
  state_network <- st_transform(state_network, projection)
}

# Convert to hourly ---------------------------------

starttime = Sys.time()
# make a cluster of available cores (minus 1 in case needed for other activities)
# nCores <- detectCores() - 1
# cl <- makeCluster(nCores, useXDR = F) 
# registerDoParallel(cl)

days <- data.frame(Day = c(1:365)) %>%
  mutate(Day = paste0('000', Day),
         Day = substr(Day, nchar(Day)-2, nchar(Day)))

hours <- data.frame(Hour = c(1:24)) %>%
  mutate(Hour = paste0('00', Hour),
         Hour = substr(Hour, nchar(Hour)-1, nchar(Hour)))

daily_for_snow <- state_network %>% # r = road
  as.data.frame() %>%
  cross_join(days) %>%
  select(osm_id, Day, geometry) %>% 
  distinct() %>% 
  st_as_sf()

training_frame_r <- state_network %>% # r = road
  as.data.frame() %>%
  group_by(osm_id) %>%
  cross_join(days) %>%
  filter(as.numeric(Day) <= 7) %>% # delete when using the whole system 
  group_by(osm_id, Day) %>%
  cross_join(hours) 

rm(days, hours)

timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "elapsed", "\n")

# Possible alternate method to create training_frame_r for each 
# of the 12 months - alternative to lines 144-158 above. New method can accomplish the
# task in 15% of the time (takes 15.69 minutes now for one year, but not yet adding
# in the residential roads). Not yet using the parallel processing, but can uncomment
# the corresponding lines at beginning and end to try that as well (173-176 and 185).
m <- 1
gc()
starttime = Sys.time()
# # start cluster for parallel processing
# nCores <- detectCores() - 1
# cl <- makeCluster(nCores, useXDR = F)
# registerDoParallel(cl)
training_frame_r <- state_network %>% as.data.frame()
for (m in 1:12){ 
  num_days = lastdays[m]
  temp = do.call(bind_rows, replicate(24*num_days, training_frame_r, simplify = FALSE)) %>% arrange(osm_id)
  month_frames[[m]] <- cbind(temp,month_frames[[m]])
}
timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "elapsed", "\n")
# stopCluster(cl); rm(cl); gc(verbose = F) # Stop the cluster

# Load Crash Data ------------------------------

# load raw file 

crash_files <- list.files(file.path(inputdir,"Crash", state_osm), pattern = 'crash.shp$', full.names = TRUE)

n = as.numeric(length(crash_files)) 
datalist <- list()
datalist = vector("list", length = n)

l = 0

if(state == "MN"){
  timestamps <- read.csv(file.path(inputdir,'Crash', state_osm, paste0(state_osm, '_timestamps.csv'))) %>%
    rename(INCIDEN = INCIDENT_ID) %>%
    mutate(DATE_TIME_OF_INCIDENT = as.POSIXct(DATE_TIME_OF_INCIDENT, format = "%m/%d/%Y %H:%M"))

  for (i in crash_files){
    l <- l + 1 
    temp <- read_sf(i) %>%
      left_join(timestamps, by = 'INCIDEN') %>%
      mutate(Year = format(DATE_TIME_OF_INCIDENT, '%Y'),
             Day = format(DATE_TIME_OF_INCIDENT, '%j'),
             Hour = format(DATE_TIME_OF_INCIDENT, '%H'),
             Weekday = weekdays(DATE_TIME_OF_INCIDENT),
             CRASH_S = as.numeric(CRASH_S),
             LIGHT_C = as.numeric(LIGHT_C),
             WORKERS = as.numeric(WORKERS)) %>%
             #print(st_crs(temp))
      st_transform(projection)
    datalist[[l]] <- temp
  }
}

if(state == "WA"){
  for (i in crash_files){
    l <- l + 1 
  temp <- read_sf(i) %>%
    mutate(TIME = ifelse(nchar(TIME) == 3, paste0(0, TIME), TIME),
           ACC_DATE = paste(ACC_DATE, " ", TIME),
           ACC_DATE = as.POSIXct(ACC_DATE, format = '%Y-%m-%d %H%M'),
           Year = format(ACC_DATE, '%Y', trim = FALSE),
           Day = format(ACC_DATE, '%j', trim = FALSE),
           Hour = format(ACC_DATE, '%H', trim = FALSE),
           Weekday = weekdays(ACC_DATE))
  
    datalist[[l]] <- temp
  }
  
}

total_crashes <- do.call(bind_rows, datalist)

total_crashes <- st_as_sf(total_crashes) %>%
  filter(Year == year) %>%
  st_transform(projection) 

joined <- st_join(total_crashes, state_network, join = st_nearest_feature)

joined <- as.data.frame(joined) %>%
  mutate(crash = 1) %>%
  select(osm_id, Day, Hour, crash) %>%
  group_by(osm_id, Day, Hour) %>%
  summarise(crash = sum(crash)) %>%
  ungroup()

training_frame_rc <- training_frame_r %>% # c is crashes
  left_join(joined, by = c('osm_id', 'Day', 'Hour')) %>%
  mutate(crash = ifelse(is.na(crash), 0, crash)) %>% 
  st_as_sf()

stopCluster(cl); rm(cl); gc(verbose = F) # Stop the cluster

timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "elapsed", "\n")

# Merging Weather ---------------------------------------------------------

source("utility/Join_Road_Weather.R")
