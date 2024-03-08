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


##identify state for analysis
state <- 'Minnesota'

#Retrieves relevant coordinates; always a rectangle
state_bbox <- getbb(state) # use nominatimlite to fix this bug 

##identify road types you'd like to query for; can pick from c('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential')
road_types <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary')

#initialize object
roadways <- list()

state_filename <- paste0(gsub(" ", "_", state),"_network")
file_path <- file.path(state, paste0(state_filename, '.gpkg'), paste0(state_filename,'.shp'))

if (file.exists(file.path(file_path))){
  .temp <- read_sf(file_path)
  assign(state_filename, .temp)
  rm(.temp)
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
  
  state_network <- total_network$osm_lines
  
  if (!(dir.exists(state))){
    dir.create(state)}
  
  state_network <- st_transform(state_network, crs = 'WGS84') 
  
# Pull state boundaries
state_maps <- states(cb = TRUE, year = 2021) %>%
  filter_state(state) %>%
  st_transform(crs = 'WGS84')

county_map <- counties(state = state, cb = TRUE, year = 2021)

#Filter out roadways outside the state

network_within_state <- st_join(state_network, state_maps, join = st_within) %>%
  filter(!is.na(NAME)) %>%
  select(osm_id, highway, ref, geometry)

write_sf(network_within_state, paste0(state, "/", state_filename, ".gpkg"), driver = "ESRI Shapefile")

assign(state_filename, network_within_state)

}

ggplot() + geom_sf(data = Minnesota_network)
# Load Crash Data ------------------------------

# load raw file 



timestamps <- read.csv(file.path(state, paste0(state, '_timestamps.csv'))) %>%
  rename(INCIDEN = INCIDENT_ID) %>%
  mutate(DATE_TIME_OF_INCIDENT = as.POSIXct(DATE_TIME_OF_INCIDENT, format = "%m/%d/%Y %H:%M"))

crash_files <- list.files(state, pattern = 'crash.shp$', full.names = TRUE)

total_crashes <- data.frame()
for (i in crash_files){
crash <- read_sf(i) %>%
  left_join(timestamps, by = 'INCIDEN') %>%
  mutate(Year = format(DATE_TIME_OF_INCIDENT, '%Y'),
         Day = format(DATE_TIME_OF_INCIDENT, '%j'),
         Hour = format(DATE_TIME_OF_INCIDENT, '%H'))
total_crashes <- plyr::rbind.fill(total_crashes, crash)
}

total_crashes <- st_as_sf(total_crashes)

joined <- st_join(total_crashes, network_within_state, join = st_nearest_feature)

joined_df <- as.data.frame(joined)

joined_test <- joined_df %>%
  group_by(osm_id, Year, Day, Hour) %>%
  summarise(num_unique_incidents = n_distinct(INCIDEN))


#crashes_clean <- crashes %>% st_zm(what="ZM") %>% sf::st_coordinates() %>% as.data.frame() # this isn't a valid shapefile but the points are unplotable without.

