# Weather data overlay on grid aggregated data.
# Working with geoTIFFs from NEXRAD, from utility/geotiff_script.py
# This step follows Grid_aggregation.R.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(rgdal)
library(rgeos)
library(raster)
library(foreach)
library(doParallel)
library(aws.s3)
library(tidyverse)

# Set hexagon size
HEXSIZE = c("1", "4", "05") #[1] Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

runmonths = c("04", "05", "06", "07", "08", "09") 

# Updating to run on ATA
codedir <- "~/SDI_Waze" 
waze.bucket <- "ata-waze"
inputdir <- "WazeEDT_RData_Input"
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


for(SIZE in HEXSIZE){
  
  # Hexagons:
  hex <- readOGR(localdir, layer = paste0("MD_hexagons_", SIZE, "mi_newExtent_newGRIDID"))
  
  # Read in county shapefile, and match all projections. See http://rspatial.org/spatial/rst/6-crs.html
  
  md_buff <- readOGR(localdir, layer = "MD_buffered")
  
  hex <- spTransform(hex, proj4string(md_buff)) # identical(crs(hex), crs(md_buff))
  
  # Clip hex to MD 
  hex.md.int <- gIntersects(md_buff, hex, byid = T) # Problem with using md.co: introduces duplicate grid IDs at county boundaries.
  hex.md <- hex[hex.md.int[,1],]
  # plot(hex.md)
  
  # Set up cluster
  avail.cores <- parallel::detectCores()
  if(avail.cores > 8 & length(runmonths) < 12) avail.cores = length(runmonths) # limit to runmonths if on r4.4xlarge
  cl <- makeCluster(avail.cores) 
  registerDoParallel(cl)
  
  # Start month loop ----
  # Big loop, over months. Process weather data inside this loop.
  starttime <- Sys.time()
  
  writeLines(c(""), paste0(SIZE, "_log.txt"))    
  
  foreach(mo = runmonths, .packages = c("raster", "dplyr", "aws.s3", "rgdal", "tidyr")) %dopar% {
  
    sink(paste0(SIZE, "_log.txt"), append=TRUE)
    
    cat(paste(Sys.time()), mo, "\n")                                                           
    
    # Waze - aggregated. s3load directly, not saving file to instance
    s3load(object = file.path(inputdir, paste0("WazeTimeEdtHexAll_", mo, "_", SIZE, "mi.RData")), bucket = waze.bucket)
    
    # Weather - localdir for now, copied above from S3 bucket (still good to edit NEXRAD script to save directly to S3)
    weatherdir <- localdir
    
    # Get grid values for hex polygons with extract(). Now working with RasterBrick instead of individual layers
    
    # Get list of weather tifs for this month
    mo.wx <- dir(weatherdir)[grep(paste0("2017-", mo), dir(weatherdir))]
    
    # Get the month, day and hour for each file
    tif_time <- vector()
    for(i in mo.wx){
      if(nchar(i) == 21){
        day <- strptime(substr(i, 1, 13), "%Y-%m-%d-%H") 
      } else {
        day <- strptime(substr(i, 1, 12), "%Y-%m-%d-%H") 
      }
      yr <- format(day, "%Y") # Numeric year value 
      mo <- format(day, "%m")
      dy <- format(day, "%j") # Numeric day value
      dy.mo <- format(day, "%d")
      hr <- format(day, "%H") # Numeric hour value 
      tif_time <- rbind(tif_time, c(filename =i, year = yr, mo = mo, jday = dy, day = dy.mo, hour = hr))
    }
    tif_time <- as.data.frame(tif_time)
    
    files.to.stack = mo.wx[tif_time$mo == mo]
    
    stack.wx <- stack(x = file.path(weatherdir, files.to.stack)) # keeps on disk
    brick.wx <- brick(stack.wx) # reads in memory. Slower to set up, faster to work with
    
    wx.b <- projectRaster(brick.wx, crs = proj4string(md_buff))
    
    w2.b <- crop(wx.b, extent(hex.md)) 
  
    hex.wx <- raster::extract(w2.b, hex.md, 
                      fun = max, na.rm = T, # summarize by taking the MAX value for the cells that intersect with this polygon
                      df = T) # return results as a data frame
    
    # Reshape to long
    wx.mo <-  hex.wx %>% 
      gather(key = filename,
             value = wx,
             colnames(hex.wx)[2]:colnames(hex.wx)[ncol(hex.wx)])
    
    names(wx.mo)[1] = "GRID_ID"
    
    tif_time$filename <- sub(".tif", "", make.names(tif_time$filename)) # to match colnames used in hex.wx.b
    
    wx.mo2 <- left_join(wx.mo, tif_time, by = "filename")    
    
    wx.mo2$wx[wx.mo2$wx == -Inf] = NA
    
    assign(paste0("wx.mo.", mo), wx.mo2) # create object for the combined output of this month
    
    # Read from previous: s3load( paste0("additional_data/weather/", mo, "_", SIZE, "_mi.RData"), bucket = waze.bucket); wx.cb = wx.mo.06 # Manually update month
    
    wte <- wazeTime.edt.hexAll
    wte$hextime <- as.character(wte$hextime)
    wte$year <- "2017" # update after fixing Grid_aggregate to include year
    wte$hour <- as.character(wte$hour)
    wx.mo2$hour <- as.character(wx.mo2$hour)
    wte$day <- as.character(wte$day)
    wx.mo2$day <- as.character(wx.mo2$day)
    wte$GRID_ID <- as.character(wte$GRID_ID)
    wx.mo2$GRID_ID <- as.character(wx.mo2$GRID_ID)
    wx.mo2$year <- as.character(wx.mo2$year)
    
    # Make sure no duplicates exist by grid ID, year, day, and hour
    stopifnot(all(!duplicated(with(wx.mo2, paste(GRID_ID, year, day, hour)))))
    
    WazeTime.edt.wx <- left_join(wte, wx.mo2[c("GRID_ID", "day", "hour", "year", "wx")],
                                 by = c("GRID_ID", "day", "hour", "year")
                                 )
  
    # Separately save wx and Grid aggregated files
    s3save(list = paste0("wx.mo.", mo), 
           object = paste0("additional_data/weather/", mo, "_", SIZE, "_mi.RData"),
           bucket = waze.bucket
    )
    
    s3save(list = "WazeTime.edt.wx", 
           object = file.path(outputdir, paste0("WazeTimeEdtHexWx_", mo, "_", SIZE, "_mi.RData")),
           bucket = waze.bucket
            )
    
    timediff <- Sys.time() - starttime
    cat(mo, "complete", round(timediff, 2), attr(timediff, "units"), "elapsed \n")       
  } # end month foreach loop
  
                    
  stopCluster(cl) # stop the cluster when done

  timediff <- Sys.time() - starttime
  cat(SIZE, "complete", round(timediff, 2), attr(timediff, "units"), "elapsed \n") 
  
} # end hexsize loop

# Check with plots

CHECKPLOT = F

library(rgdal)

gdaldir = "/home/dflynn-volpe/workingdata/" # Requires full path, no alias

if(CHECKPLOT){  
  
  co <- rgdal::readOGR(gdaldir, layer = "cb_2015_us_county_500k")
  
  # maryland FIPS = 24
  md.co <- co[co$STATEFP == 24,]
  
  pdf("Checking_Wx_overlay_newID.pdf", width = 11, height = 8)
  for(SIZE in HEXSIZE){
    # Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, 
    hex <- rgdal::readOGR(gdaldir, layer = paste0("MD_hexagons_", SIZE, "mi_newExtent_newGRIDID"))
    
    hex2 <- spTransform(hex, proj4string(md.co))
    plot(hex2)
    plot(md.co, add = T, col = "red")
    
    s3load(object = file.path(outputdir, paste0("WazeTimeEdtHexWx_", mo, "_", SIZE, "_mi.RData")),
           bucket = waze.bucket)
    
    class(WazeTime.edt.wx) <- "data.frame"
    
    # Join back to hex2, just check unique grid IDs for plotting
    w.t <- WazeTime.edt.wx[!duplicated(WazeTime.edt.wx$GRID_ID),]
    
    h.w <- hex2[hex2$GRID_ID %in% w.t$GRID_ID,]
    plot(h.w, col = "blue", add = T)
  }
  dev.off()
}
