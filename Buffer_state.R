# Buffer state polygon. Make a 0.5 mile buffer around Maryland to clip Waze events. Want 0.5 mile because that is the spatial range being used to associate EDT and Waze events.



# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(sp)
library(maps) # for mapping base layers
library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
# Version 1: from .csv aggregations made by Lia ----

wazedir <- "W:/SDI Pilot Projects/Waze/Working Documents"

setwd(wazedir)

# Read in county data from Census
counties <- readOGR("Census Files", layer = "cb_2015_us_county_500k")
md_counties <- counties[counties$STATEFP == 24,]

# Make a union for whole state
ids <- rep("MD", nrow(md_counties))
md.u <- unionSpatialPolygons(md_counties, ids)

# Width is in degrees. Conver to 0.5 mi
testx <- SpatialPoints(data.frame(lon = c(-79), lat = c(39)))
testy <- SpatialPoints(data.frame(lon = c(-78), lat = c(39)))

proj4string(testx) <- proj4string(testy) <- proj4string(md_counties)

spDists(testx, testy) # one degree distance in lat = 111.0069 miles. In Long = 86.6 miles.
# Take the one which results in a larger buffer
buffdist <- 1/86

xx <- gBuffer(md.u, width =  buffdist)

proj4string(xx) <- proj4string(md_counties)

xx <- SpatialPolygonsDataFrame(xx, data = data.frame(MD = "MDbuff", row.names = 'buffer'))

writeOGR(xx, dsn = "Census Files", layer = "MD_buffered", driver = 
           "ESRI Shapefile")
