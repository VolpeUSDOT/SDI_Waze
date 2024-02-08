library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)

##select server to query OSM api
new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)


##identify state for analysis
state <- 'Tennessee'

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

roadway <- c(roadway, list(map_data))

}

total_network <- do.call(c, roadway)

ggplot() + geom_sf(data = total_network$osm_lines, color = 'blue')




