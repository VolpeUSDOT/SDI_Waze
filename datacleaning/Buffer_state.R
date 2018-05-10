# Buffer state polygon. Make a 0.5 mile buffer around each state to clip Waze events. Want 0.5 mile because that is the spatial range being used to associate EDT and Waze events.
# Adapted for SDC use


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
# Check for package installations

codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(sp)
library(maps) # for mapping base layers
library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
user <- paste0( "/home/", system("whoami", intern = TRUE))
localdir <- paste0(user, "/workingdata/census") # full path for readOGR

setwd(localdir)

# Read in county data from Census. In EPSG:4269.
counties <- rgdal::readOGR(localdir, layer = "cb_2017_us_county_500k")

# Project to Albers equal area, ESRI 102008
proj <- showP4(showWKT("+init=epsg:102008"))

counties <- spTransform(counties, CRS(proj))

states = c("CT", "MD", "UT", "VA")
# Relevant states now: CT 09, MD 24, UT 49, VA 51
FIPS = data.frame(state = c("CT", "MD", "UT", "VA"),
                  FIPS =  c("09", "24", "49", "51"),
                  stringsAsFactors = F)

FIPS # manually confirm

for(i in states){ # i = "CT"
  
  i_counties <- counties[counties$STATEFP == FIPS$FIPS[FIPS$state == i],]
  
  # Make a union for whole state
  ids <- rep(i, nrow(i_counties))
  i.u <- unionSpatialPolygons(i_counties, ids)
  
  
  # Width is in degrees. Conver to 0.5 mi
  buffdist <- 0.5 * 1609.344 # units are meters in shapefiles
  
  xx <- gBuffer(i.u, width =  buffdist)
  
  xx <- SpatialPolygonsDataFrame(xx, data = data.frame(State = paste0(i, "buff"), row.names = 'buffer'))
  
  plot(i_counties, main = paste("Buffer", i))
  plot(xx, add = T, col = alpha("firebrick", 0.8))
  plot(i.u, add = T, col = alpha("grey80", 0.8))
  
  writeOGR(xx, dsn = localdir, layer = paste0(i, "_buffered"), driver = 
             "ESRI Shapefile")
  
}
