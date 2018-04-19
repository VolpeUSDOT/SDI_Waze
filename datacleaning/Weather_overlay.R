# Weather data overlay on grid aggregated data.
# Working with geoTIFFs from NEXRAD, from utility/geotiff_script.py
# This step follows Grid_aggregation.R.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(maps)
library(maptools)
library(sp)
library(rgdal)
library(rgeos)
library(raster)
library(foreach)
library(doParallel)
library(aws.s3)
library(tidyverse)

# Set hexagon size
HEXSIZE = c("1", "4", "05")[3] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

runmonths = c("04", "05", "06", "07", "08", "09") 
BYPOLY = F # leave as F for looping over files rather than over polygons within a file

# Updating to run on ATA
codedir <- "~/SDI_Waze" 
waze.bucket <- "ata-waze"
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- inputdir # write out to the same subfolder in this S3 bucket.
localdir <- "/home/dflynn-volpe/workingdata" # location on this instance to hold working data files that can be s3loaded. *Need full path, cannot abbreviate to ~/woringdata, for readOGR*

aws.signature::use_credentials()

source(file.path(codedir, "utility/wazefunctions.R"))

# Read in data: hexagons, and Grid-aggregated Waze data ----
# Weather data read in inside the foreach loop, need to move to EC2 instance first
# Grab any necessary data from the s3 bucket
if(length(dir(localdir)[grep("shapefiles_funClass", dir(localdir))]) == 0){
  
  s3transfer = paste("aws s3 cp s3://ata-waze/MD_hexagon_shapefiles", localdir, "--recursive --include '*'")
  system(s3transfer)
  
  for(dd in c("shapefiles_funClass.zip", "shapefiles_rac.zip", "shapefile_wac.zip")){
    uz <- paste("unzip", file.path(localdir, dd))
    system(uz)
  }
  # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
  system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
}

if(length(dir(localdir)[grep(paste0("2017-", runmonths[length(runmonths)], "-30"), dir(localdir))]) == 0){
  # Put weather data into temporary directory on EC2 instance
  s3transfer = paste("aws s3 cp s3://ata-waze/additional_data/weather", localdir, "--recursive --include '*'")
  system(s3transfer)
}

# Hexagons:
hex <- readOGR(localdir, layer = paste0("MD_hexagons_", HEXSIZE, "mi_newExtent_neighbors"))

# Read in county shapefile, and match all projections. See http://rspatial.org/spatial/rst/6-crs.html

md_buff <- readOGR(localdir, layer = "MD_buffered")

hex <- spTransform(hex, proj4string(md_buff)) # identical(crs(hex), crs(md_buff))

# Clip hex to MD 
hex.md.int <- gIntersects(md_buff, hex, byid = T) # Problem with using md.co: introduces duplicate grid IDs at county boundaries.
hex.md <- hex[hex.md.int[,1],]

# Set up cluster
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# Start month loop ----
# Big loop, over months. Process weather data inside this loop.
starttime <- Sys.time()

for(mo in runmonths){ 
  
  # Waze - aggregated. s3load directly, not saving file to instance
  s3load(object = file.path(inputdir, paste0("WazeTimeEdtHexAll_", mo, "_", HEXSIZE, "mi.RData")), bucket = waze.bucket)
  
  # Weather - localdir for now, copied above from S3 bucket (still good to edit NEXRAD script to save directly to S3)
  weatherdir <- localdir
  
  # Get grid values for hex polygons with extract(). S.l.o.w.. Parallized; consider resampling. Also consider rasterizing the poloygons and using getValues instead of extract: https://gis.stackexchange.com/questions/130522/increasing-speed-of-crop-mask-extract-raster-by-many-polygons-in-r/188834, see BYPOLY below. Also possibly faster to compile hourly to a monthly GRD and read a brick.

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
    wx <- projectRaster(wx, crs = proj4string(md_buff))
    
    # Crop the raster to the hex.md layer
    w2 <- crop(wx, extent(hex.md)) # very fast, only rectangular cropping but actually makes it smaller. 

    hex.wx <- extract(w2, hex.md, 
                      fun = max, na.rm = T, # summarize by taking the MAX value for the cells that intersect with this polygon
                      df = T) # return results as a data frame
    
    wx.mo <- data.frame(GRID_ID = hex.md$GRID_ID, year = yr, day = dy, hour = hr, wx = hex.wx[,2])
    wx.mo$wx[wx.mo$wx == -Inf] = NA
    wx.mo
  } # end foreach loop over files within month
  
  assign(paste0("wx.mo.", mo), wx.cb) # create object for the combined output of this month
  
  # Read from previous: s3load( paste0("additional_data/weather/", mo, "_", HEXSIZE, "_mi.RData"), bucket = waze.bucket); wx.cb = wx.mo.06 # Manually update month
  
  wte <- wazeTime.edt.hexAll
  wte$hextime <- as.character(wte$hextime)
  wte$year <- "2017" # update after fixing Grid_aggregate to include year
  wte$hour <- as.character(wte$hour)
  wx.cb$hour <- as.character(wx.cb$hour)
  wte$day <- as.character(wte$day)
  wx.cb$day <- as.character(wx.cb$day)
  wte$GRID_ID <- as.character(wte$GRID_ID)
  wx.cb$GRID_ID <- as.character(wx.cb$GRID_ID)
  wx.cb$year <- as.character(wx.cb$year)
  
  # Collapse wx to only unique grid ID x day x hour combinations (not a problem with new code)
  wx.cb <- wx.cb[!duplicated(with(wx.cb, paste(GRID_ID, year, day, hour))),]
  
  WazeTime.edt.wx <- left_join(wte, wx.cb,
                               by = c("GRID_ID", "day", "hour", "year")
                               )

  # Separately save wx and Grid aggregated files
  s3save(list = paste0("wx.mo.", mo), 
         object = paste0("additional_data/weather/", mo, "_", HEXSIZE, "_mi.RData"),
         bucket = waze.bucket
  )
  
  s3save(list = "WazeTime.edt.wx", 
         object = file.path(outputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")),
         bucket = waze.bucket
          )
  
  timediff <- Sys.time() - starttime
  cat(mo, "complete", round(timediff, 2), attr(timediff, "units"), "elapsed \n")       
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