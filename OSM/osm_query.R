library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)

setwd(file.path(dirname(rstudioapi::getActiveDocumentContext()$path)))

##select server to query OSM api
#If you the below doesn't work, try https://overpass.kumi.systems/api/interpreter

new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)


##identify state for analysis
state <- 'North Carolina'

#Retrieves relevant coordinates; always a rectangle
state_bbox <- getbb(state)

##identify road types you'd like to query for; can pick from c('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential')
road_types <- c('motorway', 'trunk', 'primary')

#initialize object
roadways <- list()

for (i in road_types){

map_data <- opq(bbox = state_bbox) %>%
  add_osm_feature(key = 'highway', value = i) %>%
  osmdata_sf()

roadways <- c(roadways, list(map_data))

}


total_network <- do.call(c, roadways)

ggplot() + geom_sf(data = total_network$osm_lines, color = 'blue')


nc_network <- total_network$osm_lines %>%
  select(geometry)

write_sf(nc_network, "nc_road_network/nc_expanded_network.shp")
