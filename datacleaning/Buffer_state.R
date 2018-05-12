# Buffer state polygon. Make a 0.5 mile buffer around each state to clip Waze events. Want 0.5 mile because that is the spatial range being used to associate EDT and Waze events.
# Adapted for SDC use
# Also creates hex files for each state, based of national hexagon layer and the newly-created buffered files, for faster overlay in Hex_UA_Overlay_SDC.R
# Cstack limit: must run outside of RStudio. In terminal run `ulimit -s unlimited`, then `Rscript datacleaning/Buffer_state`

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
# Check for package installations
user <- "/home/daniel" # for sourcing as root
#user <- paste0( "/home/", system("whoami", intern = TRUE))
codeloc <- file.path(user, "SDI_Waze")
# source(file.path(codeloc, 'utility/get_packages.R'))

library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
library(tidyverse)
localdir <- paste0(user, "/workingdata/census") # full path for readOGR
localdir.hex <- paste0(user, "/workingdata/Hex")

setwd(localdir)

# Read in county data from Census. In EPSG:4269.
counties <- rgdal::readOGR(localdir, layer = "cb_2017_us_county_500k")

# Read in hexagon shapefile, as .RData. This is a rectangular surface of 1 sq mi area hexagons, national for lower 48 states. Will need to prep AK and HI separately.
#load("~/workingdata/Hex/hexagons_1mi_lower48_neighbors.RData")
hex <- readOGR(file.path(localdir.hex), layer = "hexagons_1mi_lower48_neighbors")
# In terminal, ulimit -s 16384, then R readOGR, save as .RData. Producex 4.5 Gb .Rdata file.
# However! Incompletely loaded; only got bottom 1/4 of lower 48 using the termina/.RData method. Re-do.
# system("ulimit -s")

# Project to Albers equal area, ESRI 102008
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version of AEA. Use this for all projections for consistency; this is what the hexagon layer is in 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

counties <- spTransform(counties, CRS(proj.USGS))

states = c("CT", "MD", "UT", "VA")
# Relevant states now: CT 09, MD 24, UT 49, VA 51. 
FIPS = state.fips[state.fips$abb %in% states,]
FIPS = FIPS[!duplicated(FIPS$abb),]
FIPS$fips = formatC(FIPS$fips, width = 2, flag = "0")

for(i in states){ # i = "CT"
  
  i_counties <- counties[counties$STATEFP == FIPS$fips[FIPS$abb == i],]
  
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
  
  # # Make hex layer based on buffered state polygon
  # # !!! problem: national hex is in very different latitude
  # > bbox(hex)
  # min     max
  # x -2362581 2265698
  # y   258207 1023446
  # > bbox(i_counties)
  # min     max
  # x 1833457 1986024
  # y 2215510 2365566

    hex_i_intersect <- gIntersects(xx, hex, byid = T)
  hex_i <- hex[hex_i_intersect[,1],]
  #plot(hex_i)
  writeOGR(hex_i, dsn = localdir.hex, layer = paste0(i, "_Hex"), driver = 
             "ESRI Shapefile")
  
}
