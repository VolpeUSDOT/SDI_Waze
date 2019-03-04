# Read in Bellevue wikimaps. The shapefile Wikimap_raw was exported from the .mpk map package file sent by Bellevue.

library(rgdal)
library(sp)

bellevue_dir = "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Data/Bellevue"

wiki = rgdal::readOGR(file.path(bellevue_dir, "Wikimap"), layer = "Wikimap_raw")

plot(wiki)

# The data live in the @data slot of the 'wiki' object SpatialPointsDataFrame object. Some data frame operations require specifying @data, but many do not. The lat / long live in @coords slot of this object.

head(wiki)
head(wiki@data)
head(wiki@coords)

# Plotting 
library(ggmap)

# Remove one stray point with lat > 47.8 -- better, clip using Bellevue boundary shapefile

wiki = wiki[wiki@coords[,2] <= 47.8,]

# Get a base map. See ?get_stamenmap for options
map_toner_12 <- get_stamenmap(bbox = as.vector(bbox(wiki)), maptype = 'toner', zoom = 12)

ggmap(map_toner_12, extent  = "device") +
  geom_point(data = wiki@data, aes(x = wiki@coords[,1], y = wiki@coords[,2], color = kml_name, shape = kml_name)) +
  ggtitle("Plotting Bellevue Wikimap")
