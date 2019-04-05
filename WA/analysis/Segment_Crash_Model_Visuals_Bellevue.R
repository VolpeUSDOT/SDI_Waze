# Code for visualize output of Bellevue models

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

source(file.path(codeloc, 'utility/get_packages.R'))

# mapping related packages
library(maps) # for mapping base layers
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(rgeos) # gintersection
library(ggmap)
library(spatstat)

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
output.loc <- file.path(data.loc, "Segments")
visual.loc <- file.path(data.loc, "Model_visualizations")

setwd(data.loc)

# Model fit value extraction ----
# load models
model_type = "logistic_models"
out.name <- file.path(data.loc, 'Segments', paste0("Bell_",model_type,".Rdata"))
stopifnot(file.exists(out.name))

if(file.exists(out.name)){load(out.name)}

# m08 logistic regression using all variables
m08 <- logistic_models[[9]]

w.all$m08_fit <- predict(m08, type = "response")

out.put <- w.all[, c("RDSEG_ID", "segtime", "m08_fit", "uniqueCrashreports")]
out.put <- out.put %>% group_by(RDSEG_ID) %>% summarize(m08_fit = sum(m08_fit),
                                                        uniqueCrashreports = sum(uniqueCrashreports))

# Dan: I suggest we split out the visualization into separate scripts for simplicity, and have the analysis script end by saving a .RData of all the model outputs only.
# Jessie: good idea, this is just a temporary space for an quick examination of the visuals. if we think R could make some good visuals, we'll use a separate code to store these.

roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
names(roadnettb_snapped)

roadnettb_snapped@data <- roadnettb_snapped@data %>% left_join(out.put, by = c("RDSEG_ID")) # adding model output to the spatial data frame

plot(roadnettb_snapped, col = roadnettb_snapped@data$m08_fit)
# legend("bottomright") # To do add legend, right now no idea of the color.

# ggmaps----
# Get a map for plotting, need to define bounding box (bbox) defined in decimal degree lat long. This is what get_stamenmaps requires. Can also make a buffer around the road network to ensure we have a large enough area to cover the whole city. Here just using road network.
state = "WA"

model_out_road = spTransform(roadnettb_snapped, CRS =  CRS("+proj=longlat +datum=WGS84"))

bbox.bell = bbox(model_out_road) # Can also use extend.range to get a larger bouding box

if(length(grep(paste0(state, "_Bellevue_Basemaps"), dir(file.path(output.loc)))) == 0){
  map_terrain_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'terrain', zoom = 14)
  map_toner_hybrid_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'toner-hybrid', zoom = 14)
  map_toner_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'toner', zoom = 14)
  save(list = c("map_terrain_14", "map_toner_hybrid_14", "map_toner_14"),
       file = file.path(output.loc, paste0(state, "_Bellevue_Basemaps.RData")))
} else { load(file.path(output.loc, paste0(state, "_Bellevue_Basemaps.RData"))) }


# map <- qmap('Bellevue', zoom = 12, maptype = 'hybrid')

# To use geom_path, we need to extract the lat-long start and end for every segment.
# this is probably not worth it, let's just export and plot in Tableau / ArcGIS
# Jessie: make sense. I'll export the layer for mapping in ArcGIS.
# https://stackoverflow.com/questions/32413190/how-to-plot-spatiallinesdataframe-feature-map-over-google-maps

NOTRUN = F

if(NOTRUN){
  
  ggmap(map_toner_hybrid_14, extent = 'device') + 
    geom_path(data = model_out_road,
              aes(mapping = aes(x = long, y = lat, group = RDSEG_ID)), 
              size=2)
}