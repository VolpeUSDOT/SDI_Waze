# Background ----------------------------

library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)

rm(list=ls()) # clear enviroment

setwd(file.path(dirname(rstudioapi::getActiveDocumentContext()$path)))

# Load Road Data ----------------------

##select server to query OSM api
#If you the below doesn't work, try https://overpass.kumi.systems/api/interpreter

new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)


##identify state for analysis
state <- 'North Carolina'

#Retrieves relevant coordinates; always a rectangle
state_bbox <- getbb(state) # use nominatimlite to fix this bug 

##identify road types you'd like to query for; can pick from c('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential')
road_types <- c('motorway', 'trunk', 'primary')

#initialize object
roadways <- list()

if (file.exists("nc_road_network/nc_expanded_network.shp")){
  nc_network <- read_sf("nc_road_network/nc_expanded_network.shp")
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


nc_network <- total_network$osm_lines %>%
  select(geometry)

write_sf(nc_network, "nc_road_network/nc_expanded_network.shp")
}

ggplot() + geom_sf(data = nc_network) # plot roads in NC Block 

# Load Crash Data ------------------------------

# load raw file 
crashes <- read_sf("2023-53/Shapefiles/nc19crash.shp")

crashes_clean <- crashes %>% st_zm(what="ZM") %>% sf::st_coordinates() %>% as.data.frame() # this isn't a valid shapefile but the points are unplotable without.
ggplot() + geom_sf(data = crashes_clean) # plot crashes in NC




