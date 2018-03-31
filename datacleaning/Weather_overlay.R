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
runmonths = c("04", "05", "06")
BYPOLY = F # leave as F for looping over files rather than over polygons within a file

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

# Read in data: hexagons, and Grid-aggregated Waze data ----
# Weather data read in inside the foreach loop
# Hexagons:
hex <- readOGR(file.path(volpewazedir, "Data/MD_hexagons_shapefiles"), layer = paste0("MD_hexagons_", HEXSIZE, "mi_newExtent_neighbors"))

# Read in county shapefile, and match all projections. See http://rspatial.org/spatial/rst/6-crs.html
co <- readOGR(file.path(wazedir, "Working Documents/Census Files"), layer = "cb_2015_us_county_500k")

# maryland FIPS = 24
md.co <- co[co$STATEFP == 24,]

hex <- spTransform(hex, proj4string(md.co)) # identical(crs(hex), crs(md.co))

# Clip hex to MD 
hex.md <- intersect(hex, md.co)

# Set up cluster
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# Start month loop ----
# Big loop, over months. Process weather data inside this loop.
for(mo in runmonths){  # mo = "04"
  
  # Waze - aggregated
  wazedatamonth <- dir(wazemonthdir)[grep(paste0("WazeTimeEdtHexAll_", mo), dir(wazemonthdir))]
  
  load(file.path(wazemonthdir, wazedatamonth))
  
  # Weather
  weatherdir <- file.path(volpewazedir, "Data/Weather_Geotiffs")
  
  # Get grid values for hex polygons with extract(). S.l.o.w.. Consider resampling and parallelizing. Also consider rasterizing the poloygons and using getValues instead of extract: https://gis.stackexchange.com/questions/130522/increasing-speed-of-crop-mask-extract-raster-by-many-polygons-in-r/188834

  # Parallize by file:----
  # Get list of weather tifs for this month
  mo.wx <- dir(weatherdir)[grep(paste0("2017-", mo), dir(weatherdir))]
  
  wx.cb <- foreach(i = 1:length(mo.wx), .combine = rbind, .packages = "raster") %dopar% {
    
    # Get the hour and day for this file
    if(nchar(mo.wx[i]) == 21){
      day <- strptime(substr(mo.wx[i], 1, 13), "%Y-%m-%d-%H") 
    } else {
      day <- strptime(substr(mo.wx[i], 1, 12), "%Y-%m-%d-%H") 
    }
    yr <- format(day, "%Y") # Numeric year value 
    dy <- format(day, "%j") # Numeric day value
    hr <- format(day, "%H") # Numeric hour value 
    
    # read in weather data from GeoTIFF. Reads in as class RasterLayer, with dimensions and resolution set from the write-out procecure. Comes with a coordiante reference system (CRS) as well, in wx@crs
    wx <- raster(file.path(weatherdir, mo.wx[i]))
    wx <- projectRaster(wx, crs = proj4string(md.co))
    
    # Crop the raster to the hex.md layer
    w2 <- crop(wx, extent(hex.md)) # very fast, only rectangular cropping but actually makes it smaller. 

    hex.wx <- extract(w2, hex.md, 
                      fun = max, na.rm = T, # summarize by taking the MAX value for the cells that intersect with this polygon
                      df = T) # return results as a data frame
    
    wx.mo <- data.frame(GRID_ID = hex.md$GRID_ID, yr, dy, hr, wx = hex.wx[,2])
    
  } # end foreach loop over files within month
  
  assign(paste0("wx.mo.", mo), wx.mo) # create object for this month
  
  WazeTime.edt.wx <- left_join(wazeTime.edt.hexAll, wx.mo)

  save(list = c("wx.mo", "WazeTime.edt.wx"), file = file.path(wazemonthdir, paste0(paste0("WazeTimeEdtHexWx_", mo))))
         
} # end month loop

                    
stopCluster(cl) # stop the cluster when done


if(BYPOLY){
  poly.list <- hex.md@polygons
  
  # Parallize by polygon. Reduces time by ~ Alternative is to continue using extract, and parallize by file
  system.time(hex.wx.fe <- foreach(i = 1:length(poly.list), .combine = rbind, .packages = "raster") %dopar% {
    single = hex.md[i,]
    cl1 = crop(wx, extent(single))
    cl2 = rasterize(single, cl1, mask = T)
    ext = getValues(cl2)
    
    } 
  )
  
  # w2 <- mask(wx, md.co) # slow, and does not speed up the extraction process, only sets masked values to NA.  
} # end by polygon 