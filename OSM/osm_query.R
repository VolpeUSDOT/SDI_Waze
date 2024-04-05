# Background ----------------------------

library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)
library(tigris)

rm(list=ls()) # clear enviroment

setwd(file.path(dirname(rstudioapi::getActiveDocumentContext()$path)))

# Identify state for analysis; Need to specify 'Washington State' for Washington
state <- "WA" 

state_osm <- ifelse(state == "WA", 'Washington State',
                    ifelse(state == "MN", "Minnesota", NA))

state_bbox <- getbb(state_osm) # Retrieves relevant coordinates; always a rectangle

state_osm <- gsub(" ", "_", state_osm) # Normalize state name

# Projection 
projection <- 5070 

# Year
year <- 2019

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
file_path <- file.path('States', state_osm, paste0(network_file, '.gpkg'), paste0(network_file,'.shp'))

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
rm(state_border)

#Filter out roadways outside the state

state_network <- st_join(total_network, state_maps, join = st_within) %>%
  filter(!is.na(NAME)) %>%
  select(osm_id, highway, ref, geometry)

write_sf(state_network, paste0('States', '/', state_osm, "/", state_osm, "_network.gpkg"), driver = "ESRI Shapefile")


}


ggplot() + geom_sf(data = state_network)

#Transform state_network crs to NAD83 before joining with crash_files; should probably change this in the files we have and bring this into the query loop

if(st_crs(state_network) != projection){
  state_network <- st_transform(state_network, projection)
}

# Convert to hourly ---------------------------------

days <- data.frame(Day = c(1:365)) %>%
  mutate(Day = paste0('000', Day),
         Day = substr(Day, nchar(Day)-2, nchar(Day)))

hours <- data.frame(Hour = c(1:24)) %>%
  mutate(Hour = paste0('00', Hour),
         Hour = substr(Hour, nchar(Hour)-1, nchar(Hour)))

training_frame_r <- state_network %>% # r = road
  as.data.frame() %>%
  group_by(osm_id) %>%
  cross_join(days) %>%
  filter(Day == '001') %>% # delete when using the whole system 
  group_by(osm_id, Day) %>%
  cross_join(hours) 

rm(days, hours)

# Load Crash Data ------------------------------

# load raw file 

crash_files <- list.files(file.path('States', state_osm, 'Crashes'), pattern = 'crash.shp$', full.names = TRUE)

n = as.numeric(length(crash_files)) 
datalist <- list()
datalist = vector("list", length = n)

l = 0

if(state == "MN"){
timestamps <- read.csv(file.path('States', state_osm, paste0(state_osm, '_timestamps.csv'))) %>%
  rename(INCIDEN = INCIDENT_ID) %>%
  mutate(DATE_TIME_OF_INCIDENT = as.POSIXct(DATE_TIME_OF_INCIDENT, format = "%m/%d/%Y %H:%M"))

for (i in crash_files){
  l <- l + 1 
temp <- read_sf(i) %>%
  left_join(timestamps, by = 'INCIDEN') %>%
  mutate(Year = format(DATE_TIME_OF_INCIDENT, '%Y'),
         Day = format(DATE_TIME_OF_INCIDENT, '%j'),
         Hour = format(DATE_TIME_OF_INCIDENT, '%H'),
         Weekday = weekdays(DATE_TIME_OF_INCIDENT))

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
  filter(Year == '2019') %>%
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

# Merging Weather ---------------------------------------------------------

source("Join_Road_Weather.R")
