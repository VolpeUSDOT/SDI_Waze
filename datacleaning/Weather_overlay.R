# Weather data overlay on grid aggregated data.
# Working with geoTIFFs from NEXRAD, from utility/geotiff_script.py
# This step follows Grid_aggregation.R

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(maps)
library(sp)
library(rgdal)
library(rgeos)
library(raster)
library(foreach)
library(doParallel)

# Set hexagon size
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
# starting now with April 1 2017
testmonth = "04"

#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                       "WazeEDT Agg",HEXSIZE,"mile Rdata Input")
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                    "WazeEDT Agg",HEXSIZE,"mile Rdata Input")


source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function
setwd(wazedir)

# Read in data: weather, hexagons, and Grid-aggregated Waze data ----

# Hexagons:
hex <- readOGR(file.path(volpewazedir, "Data/MD_hexagons_shapefiles"), layer = paste0("MD_hexagons_", HEXSIZE, "mi_newExtent_neighbors"))

# Waze
# Read in aggregated data for this month
# WazeHexTimeList vs WazeTimeEdtHexAll? Larger files are TimeList, use those
wazedatamonth <- dir(wazemonthdir)[grep(paste0("WazeTimeEdtHexAll_", testmonth), dir(wazemonthdir))]

load(file.path(wazemonthdir, wazedatamonth))

# Weather

weatherdir <- file.path(volpewazedir, "Data/Weather_Geotiffs")

testfile <- dir(weatherdir)[grep(paste0("2017-", testmonth), dir(weatherdir))][1]

# Get the hour and day for this file
if(nchar(testfile) == 21){
  day <- strptime(substr(testfile, 1, 13), "%Y-%m-%d-%H") 
  } else {
  day <- strptime(substr(testfile, 1, 12), "%Y-%m-%d-%H") 
  }
dy <- format(day, "%j") # Numeric day value
hr <- format(day, "%H") # Numeric hour value 

# read in weather data from GeoTIFF. Reads in as class RasterLayer, with dimensions and resolution set from the write-out procecure. Comes with a coordiante reference system (CRS) as well, in wx@crs
wx <- raster(file.path(weatherdir, testfile))

# Read in county shapefile, and match all projections. See http://rspatial.org/spatial/rst/6-crs.html
co <- readOGR(file.path(wazedir, "Working Documents/Census Files"), layer = "cb_2015_us_county_500k")

# maryland FIPS = 24
md.co <- co[co$STATEFP == 24,]

hex <- spTransform(hex, proj4string(md.co))
wx <- projectRaster(wx, crs = proj4string(md.co))

# identical(crs(wx), crs(hex));identical(crs(wx), crs(md.co))

# Grid extraction ----

# overlay the raster and the hex layers, extracting the raster values to hex for this time period
# First clip hex to MD 
hex.md <- intersect(hex, md.co)

# Crop the raster to the hex.md layer
w2 <- crop(wx, extent(hex.md)) # very fast, only rectangular cropping but actually makes it smaller. 
# w2 <- mask(wx, md.co) # slow, and does not speed up the extraction process, only sets masked values to NA.

# plot(hex.md)
# plot(wx, add = T)
# plot(w2, add = T)


# Get grid values for hex polygons with extract(). S.l.o.w.. Consider resampling and parallelizing. Also consider rasterizing the poloygons and using getValues instead of extract: https://gis.stackexchange.com/questions/130522/increasing-speed-of-crop-mask-extract-raster-by-many-polygons-in-r/188834

# 178.37

system.time(hex.wx <- extract(w2, hex.md, 
                              fun = max, na.rm = T, # summarize by taking the MAX value for the cells that intersect with this polygon
                              df = T) # return results as a data frame
            )

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

poly.list <- hex.md@polygons

hex.wx <- foreach(i = 1:length(poly.list), .combine = rbind, .packages = "raster") %dopar% {
                  single = poly.list[[i]]
                  cl1 = crop(wx, extent(single))
                  cl2 = rasterize(single, cl1, mask = T)
                  ext = getValues(cl2)
                  
                  } 
  
  extract(wx, hex.md, 
                  fun = max, na.rm = T, # summarize by taking the MAX value for the cells that intersect with this polygon
                  df = T) # return results as a data frame
                  
                  
                  
stopCluster(cl) # stop the cluster when done