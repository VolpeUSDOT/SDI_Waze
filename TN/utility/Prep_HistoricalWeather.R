# Prepare historical weather for random forest work

# Run from RandomForest_WazeGrid_TN.R
# already in memory are localdir and g, which repreresents the grid type to use
# Next step after this script is to run append.hex function to add 
censusdir <- paste0(user, "/workingdata/census") # full path for readOGR, for buffered state shapefile created in first step of data pipeline, ReduceWaze_SDC.R


proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Check to see if these processing steps have been done yet; load from prepared file if so
prepname = paste("Prepared", "Weather", g, sep="_")

if(length(grep(prepname, dir(file.path(localdir, "Weather")))) == 0) { 
  library(gstat) # For kriging
  library(raster) # masks several functions from dplyr, use caution
  library(doParallel)
  library(foreach)
  
  cat("Preparing", "Weather", g, "\n")
  
  # Read in GHCN data
  wx.files <- dir(file.path(localdir, "Weather", "GHCN"))
  wx <- vector()
  
  # Most stations don't have most of the wx variables. Simplify to core variables, per GHCN documentation:
  core_vars = c("STATION", "NAME", "DATE", 
                "PRCP", "SNOW", "SNWD", "TMAX", "TMIN")
  # longer set of possible variables
  # vars = c("STATION", "NAME", "DATE", "AWND", "DAPR", "MDPR", "PGTM", "PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "TOBS", "WSFG", "WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT08", "WT10", "WT11")

  for(k in wx.files){ # k = wx.files[1]
    if(length(grep('stations', k))==0){
      wxx <- read.csv(file.path(localdir, "Weather", "GHCN", k))
      wx <- rbind(wx, wxx[core_vars])
      rm(wxx)
    }
  }

  station_file <- file.path(localdir, "Weather", "GHCN", wx.files[grep('stations', wx.files)])
  stations <- read_fwf(station_file,
                       col_positions = fwf_empty(station_file))
  names(stations) = c("STATION", "lat", "lon", "masl", "NAME", "x1", "x2", "x3")
  
  wx$DATE <- as.Date(as.character(wx$DATE))
  
  wx <- wx[order(wx$DATE, wx$STATION),]
  
  # Make sure values are reasonable
  # range(wx$PRCP, na.rm=T)
  # wx %>%
  #   filter(TMAX < 10) # Looks possible
  # wx %>%
  #   filter(TMIN < -10) # Replace -99 with NA
   
  wx$TMIN[wx$TMIN == -99] = NA
  wx$TMAX[wx$TMAX == -99] = NA

  # To match with forecast, want the following by day
  # date: yday, hour
  # high: fahrenheit
  # low: fahrenheit
  # precip
  # snow_allday: in

  wx <- left_join(wx, stations[1:5], by = "STATION")
  
  # Interpolate over state ----
  # http://rspatial.org/analsis/rst/4-interpolation.html
  # First, make this a spatial points data frame
  # Project crashes
  wx.proj <- SpatialPointsDataFrame(wx[c("lon", "lat")], 
                                       wx,
                                       proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  wx.proj <- spTransform(wx.proj, CRS(proj.USGS))
  
  # Read in grid
  grid_shp <- rgdal::readOGR(file.path(localdir, "Shapefiles"), layer = g)
  grid_shp <- spTransform(grid_shp, CRS(proj.USGS))
  
  # Read in buffered state shapefile
  tn_buff <- readOGR(censusdir, layer = "TN_buffered")
  tn_buff <- spTransform(tn_buff, CRS(proj.USGS))
  
  # Clip grid to county shapefile
  grid_shp2 <- gIntersection(grid_shp, tn_buff, byid = T)
  
  wx <- wx %>%
    mutate(mo = format(DATE, "%m"))
  
  # Plot average Jan and June max temp by station to check
  source(file.path(codeloc, 'TN', 'datacleaning', 'Plot_weather_points.R'))
  
  # Options: nearest neighbor interpolation, inverse distance weighted, ordinary kriging...
  # Will make one raster for each variable of interest, per day, and then apply to grid/hour.
  # Here use kriging from gstat. Models are all based on spatial variance of the target variabel
  
  # Kriging. Intercept only = ordinary kriging.
  cl <- makeCluster(parallel::detectCores())
  registerDoParallel(cl)
  
  # Create an empty raster grid for the state; we will interpolate over this grid, then assign values from the raster to each grid cell. Increase n for smaller raster cells (more time-intensive)
  grd <- as.data.frame(spsample(grid_shp, 'regular', n = 10000))
  names(grd) <- c("X", "Y")
  coordinates(grd) <- c("X", "Y")
  gridded(grd) <- TRUE # for SpatialPixel
  fullgrid(grd) <- TRUE # for SpatialGrid
  proj4string(grd) <- proj4string(grid_shp)
  
  StartTime <- Sys.time()
  
  writeLines(c(""), paste0("Prep_Weather_", g, "_log.txt"))    
  
  # Start parallel loop ----
  
  wx.grd.day <- foreach(day = unique(wx$DATE), 
                      .packages = c('raster','gstat','dplyr','rgdal'), 
                      .combine = rbind) %dopar% {
    # day = unique(wx$DATE)[1]
    sink(paste0("Prep_Weather_", g, "_log.txt"), append=TRUE)
                        
    cat(paste(Sys.time()), day, "\n") 
                        
    wx.day = wx.proj[wx.proj$DATE == day,]
                        
    f.p <- as.formula(PRCP ~ 1)
    
    vg_prcp <- gstat::variogram(PRCP ~ 1, locations = wx.day[!is.na(wx.day$PRCP),])
    dat.fit <- fit.variogram(vg_prcp, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    # plot(vg_prcp, dat.fit) # Plot the semi variogram. 
    dat.krg.prcp <- krige(f.p, wx.day[!is.na(wx.day$PRCP),], grd, dat.fit)
    
    # Rasterize
    prcp_r <- raster::raster(dat.krg.prcp)
    prcp_r <- mask(prcp_r, grid_shp)
    # plot(prcp_r,  
    #      main = "Map of precipitation on 2017-04-01")
     
    # Plot the 95% confidence intervals
    # prcp_ci <- sqrt(raster(dat.krg.prcp, layer = 'var1.var')) * 1.96
    # prcp_ci <- mask(prcp_ci, grid_shp)
    # plot(prcp_ci, main = "95% CI map of precipitation on 2017-04-01",
    #      sub = "Smaller values = greater confidence")
    
    # Now do tmin, tmax, and snow
    f.tmin <- as.formula(TMIN ~ 1)
    vg_tmin <- variogram(TMIN ~ 1, wx.day[!is.na(wx.day$TMIN),])
    dat.fit <- fit.variogram(vg_tmin, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.tmin <- krige(f.tmin, wx.day[!is.na(wx.day$TMIN),], grd, dat.fit)
    tmin_r <- raster::raster(dat.krg.tmin)
    tmin_r <- mask(tmin_r, grid_shp)
    
    f.tmax <- as.formula(TMAX ~ 1)
    vg_tmax <- variogram(TMAX ~ 1, wx.day[!is.na(wx.day$TMAX),])
    dat.fit <- fit.variogram(vg_tmax, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.tmax <- krige(f.tmax, wx.day[!is.na(wx.day$TMAX),], grd, dat.fit)
    tmax_r <- raster::raster(dat.krg.tmax)
    tmax_r <- mask(tmax_r, grid_shp)
    
    f.snow <- as.formula(SNOW ~ 1)
    vg_snow <- variogram(SNOW ~ 1, wx.day[!is.na(wx.day$SNOW),])
    dat.fit <- fit.variogram(vg_snow, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.snow <- krige(f.snow, wx.day[!is.na(wx.day$SNOW),], grd, dat.fit)
    snow_r <- raster::raster(dat.krg.snow)
    snow_r <- mask(snow_r, grid_shp)
    
    # Apply to grid cells in year-day ----
    
    # Trying to speed up with rasterize
    clip1 <- crop(prcp_r, extent(grid_shp))
    grid_rst <- rasterize(grid_shp, clip1, mask = T) # still slow, but faster than extract
    prcp_extr_r <- getValues(grid_rst) # instant
    
    # Need to extract values from the raster layers to the polygons
    prcp_extr <- raster::extract(x = prcp_r,   # Raster object
                                 y = grid_shp, # SpatialPolygons
                                 fun = mean,
                                 df = TRUE)
    names(prcp_extr)[2] = "PRCP"
    daily_result <- data.frame(day, prcp_extr)
    
    tmin_extr <- raster::extract(x = tmin_r,   # Raster object
                                 y = grid_shp, # SpatialPolygons
                                 fun = mean,
                                 df = TRUE)
    names(tmin_extr)[2] = "TMIN"
    
    daily_result <- full_join(daily_result, tmin_extr)
    
    tmax_extr <- raster::extract(x = tmax_r,   # Raster object
                                 y = grid_shp, # SpatialPolygons
                                 fun = mean,
                                 df = TRUE)
    names(tmax_extr)[2] = "TMAX"
    
    daily_result <- full_join(daily_result, tmax_extr)
    
    snow_extr <- raster::extract(x = snow_r,   # Raster object
                                 y = grid_shp, # SpatialPolygons
                                 fun = mean,
                                 df = TRUE)
    names(snow_extr)[2] = "SNOW"
    
    daily_result <- full_join(daily_result, snow_extr)
    
    # Save to S3 as temporary location in case the process is interrupted
    fn = paste("Prep_Weather_Daily_", day,"_", g, ".RData", sep="")
    
    save(list="wazeTime.tn.hexAll", file = file.path(temp.outputdir, fn))
    
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, fn),
                 file.path(teambucket, state, fn)))
    
    EndTime <- Sys.time()-StartTime
    cat(day, 'completed', round(EndTime, 2), attr(EndTime, 'units'), '\n')
    
    daily_result
  } # end parallel loop
   
  
  # Plot gridded versions of same point maps to check
  source(file.path(codeloc, 'TN', 'datacleaning', 'Plot_weather_gridded.R'))
  
  EndTime <- Sys.time() - StartTime
  cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
   
  save(list = c("wx.grd.day"), 
       file = file.path(localdir, "Weather", paste0(prepname, ".RData")))

  # Copy to S3
  system(paste("aws s3 cp",
               file.path(localdir, "Weather", paste0(prepname, ".RData")),
               file.path(teambucket, state, "Weather", paste0(prepname, ".RData"))))

  } else {
  load(file.path(localdir, "Weather", paste0(prepname, ".RData")))
}

