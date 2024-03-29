# Background ----------------------------

library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)
library(tigris)

rm(list=ls()) # clear enviroment

setwd(file.path(dirname(rstudioapi::getActiveDocumentContext()$path)))

# Load Road Data ----------------------

##select server to query OSM api
#If the below doesn't work, try https://overpass.kumi.systems/api/interpreter

new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)


##identify state for analysis; Need to specify 'Washington State' for Washington
state <- 'Washington State'

#Retrieves relevant coordinates; always a rectangle
state_bbox <- getbb(state) # use nominatimlite to fix this bug 

#normalize state name
state <- gsub(" ", "_", state)

##identify road types you'd like to query for; can pick from c('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential')
road_types <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary')

#initialize object
roadways <- list()

network_file <- paste0(state,"_network")
file_path <- file.path('States', state, paste0(network_file, '.gpkg'), paste0(network_file,'.shp'))

if (file.exists(file.path(file_path))){
  state_network <- read_sf(file_path)
  print("File Found")
} else{
  
  for (i in road_types){
    print("File Not Found")
    map_data <- opq(bbox = state_bbox) %>%
      add_osm_feature(key = 'highway', value = i) %>%
      osmdata_sf()
    
    roadways <- c(roadways, list(map_data))
    
  }
  
  total_network <- do.call(c, roadways)
  
  total_network <- total_network$osm_lines
  
  if (!(dir.exists(state))){
    dir.create(state)}
  
  total_network <- st_transform(total_network, crs = 'WGS84') 
  
# Pull state boundaries
  if (state == 'Washington_State'){
    state_border <- 'Washington'
  }else{
    state_border <- state
  }
state_maps <- states(cb = TRUE, year = 2021) %>%
  filter_state(state) %>%
  st_transform(crs = 'WGS84')
rm(state_border)

#Filter out roadways outside the state

state_network <- st_join(total_network, state_maps, join = st_within) %>%
  filter(!is.na(NAME)) %>%
  select(osm_id, highway, ref, geometry)

write_sf(state_network, paste0('States', '/', state, "/", state, "_network.gpkg"), driver = "ESRI Shapefile")


}


ggplot() + geom_sf(data = state_network)

# Convert to hourly ---------------------------------

days <- data.frame(Day = c(1:365)) %>%
  mutate(Day = paste0('000', Day),
         Day = substr(Day, nchar(Day)-2, nchar(Day)))

hours <- data.frame(Hour = c(1:24)) %>%
  mutate(Hour = paste0('00', Hour),
         Hour = substr(Hour, nchar(Hour)-1, nchar(Hour)))

hourly_network <- state_network %>%
  as.data.frame() %>%
  group_by(osm_id) %>%
  cross_join(days) %>%
  filter(Day == '001') %>%
  group_by(osm_id, Day) %>%
  cross_join(hours) 

rm(days, hours)

# write_sf(hourly_network, file.path(state, 'jan_01_hourly_TN_network.shp'))

# Load Crash Data ------------------------------

# load raw file 

crash_files <- list.files(file.path('States', state, 'Crashes'), pattern = 'crash.shp$', full.names = TRUE)

if(state == 'Minnesota'){
timestamps <- read.csv(file.path(state, paste0(state, '_timestamps.csv'))) %>%
  rename(INCIDEN = INCIDENT_ID) %>%
  mutate(DATE_TIME_OF_INCIDENT = as.POSIXct(DATE_TIME_OF_INCIDENT, format = "%m/%d/%Y %H:%M"))

total_crashes <- data.frame()

for (i in crash_files){
temp <- read_sf(i) %>%
  left_join(timestamps, by = 'INCIDEN') %>%
  mutate(Year = format(DATE_TIME_OF_INCIDENT, '%Y'),
         Day = format(DATE_TIME_OF_INCIDENT, '%j'),
         Hour = format(DATE_TIME_OF_INCIDENT, '%H'),
         Weekday = weekdays(DATE_TIME_OF_INCIDENT))
total_crashes <- plyr::rbind.fill(total_crashes, temp)
  }
}

if(state == 'Washington_State'){
  total_crashes <- data.frame()
  for (i in crash_files){
  temp <- read_sf(i) %>%
    mutate(TIME = ifelse(nchar(TIME) == 3, paste0(0, TIME), TIME),
           ACC_DATE = paste(ACC_DATE, " ", TIME),
           ACC_DATE = as.POSIXct(ACC_DATE, format = '%Y-%m-%d %H%M'),
           Year = format(ACC_DATE, '%Y', trim = FALSE),
           Day = format(ACC_DATE, '%j', trim = FALSE),
           Hour = format(ACC_DATE, '%H', trim = FALSE),
           Weekday = weekdays(ACC_DATE))
  
    total_crashes <- plyr::rbind.fill(total_crashes, temp)
  }
  

}

total_crashes <- st_as_sf(total_crashes) %>%
  filter(Year == '2019') %>%
  st_transform('WGS84') 
  

joined <- st_join(total_crashes, state_network, join = st_nearest_feature)

joined <- as.data.frame(joined) %>%
  mutate(crash = 1) %>%
  select(osm_id, Day, Hour, crash) %>%
  group_by(osm_id, Day, Hour) %>%
  summarise(crash = sum(crash)) %>%
  ungroup()

training_frame <- hourly_network %>%
  left_join(joined, by = c('osm_id', 'Day', 'Hour')) %>%
  mutate(crash = ifelse(is.na(crash), 0, crash))


#crashes_clean <- crashes %>% st_zm(what="ZM") %>% sf::st_coordinates() %>% as.data.frame() # this isn't a valid shapefile but the points are unplotable without.

