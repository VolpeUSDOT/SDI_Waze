# Prepare historical weather for random forest work - Run on SDC
# Based on Prep_HistoricalWeather.R for gridded data in Tennessee, main difference is that this will extract to road segments rather than extracting to grid

# Setup  ----
user <- paste0( "/home/", system("whoami", intern = TRUE)) 
state = "WA"
localdir <- file.path(user, "workingdata", state) # full path for readOGR
use.tz <- "US/Pacific"
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
codeloc = '~/' #replace with directory to code
setwd(localdir)
# Ensure all data are in place
source(file.path(codeloc, 'utility', 'Workstation_setup.R'))

temp.outputdir <- "~/tempout" # to hold daily output files as they are generated, and then sent to team bucket

proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Load data and process ----

# Check to see if these processing steps have been done yet; load from prepared file if so
prepname = paste("Prepared", "Weather", "Bellevue", sep="_")

if(length(grep(prepname, dir(file.path(localdir, "Weather")))) == 0) { 
  library(gstat) # For kriging
  library(raster) # masks several functions from dplyr, use caution
  library(doParallel)
  library(foreach)
  library(tidyverse)
  
  cat("Preparing Bellevue Weather \n")
  
  # Read in GHCN data
  wx.files <- dir(file.path(localdir, "Weather"))
  wx.files = wx.files[grep('.(zip|RData)$', wx.files, invert = T)] # Exclude zip or RData files
  
  wx <- vector()
  
  # Most stations don't have most of the wx variables. Simplify to core variables, per GHCN documentation:
  core_vars = c("STATION", "NAME", "DATE", 
                "PRCP", "SNOW", "SNWD", "TMAX", "TMIN")
  # longer set of possible variables
  # vars = c("STATION", "NAME", "DATE", "AWND", "DAPR", "MDPR", "PGTM", "PRCP", "SNOW", "SNWD", "TAVG", 
  # "TMAX", "TMIN", "TOBS", "WSFG", "WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT08", "WT10", "WT11")

  for(k in wx.files){
    if(length(grep('stations', k))==0){
      wxx <- read.csv(file.path(localdir, "Weather", k))
      wx <- rbind(wx, wxx[core_vars])
      rm(wxx)
    }
  }

  station_file <- file.path(localdir, "Weather", wx.files[grep('stations', wx.files)])
  stations <- readr::read_fwf(station_file,
                       col_positions = readr::fwf_empty(station_file))
  names(stations) = c("STATION", "lat", "lon", "masl", "NAME", "x1", "x2", "x3")
  
  wx$DATE <- as.Date(as.character(wx$DATE))
  
  wx <- wx[order(wx$DATE, wx$STATION),]

  wx$TMIN[wx$TMIN == -99] = NA
  wx$TMAX[wx$TMAX == -99] = NA

  wx <- left_join(wx, stations[1:5], by = "STATION")
  # ensure longitudes are in the right hemisphere
  wx$lon <- sapply(wx$lon, function(x) ifelse(x > 0, x*-1, x))
  
  # Interpolate over state ----
  # http://rspatial.org/analsis/rst/4-interpolation.html
  # First, make this a spatial points data frame
  # Project crashes
  wx.proj <- SpatialPointsDataFrame(wx[c("lon", "lat")], 
                                       wx,
                                       proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  wx.proj <- spTransform(wx.proj, CRS(proj.USGS))
  
  # Read in road network
  rd_shp <- rgdal::readOGR(file.path(localdir, "Roadway", "OSTSafetyInitiative_20181121"), layer = "RoadNetwork")
  rd_shp <- spTransform(rd_shp, CRS(proj.USGS)) 
  
  
  wx <- wx %>%
    mutate(mo = format(DATE, "%m"))

  # Plot average Jan and June max temp, and annual Precipitation by station to check
  source(file.path(codeloc, 'WA', 'datacleaning', 'Plot_weather_points.R'))
  
  # Omit precipitation values for stations which have less than 1 in total for the whole year. 4 stations with bad precip values.
  wx[wx$STATION %in% bad.precip.stations$STATION,'PRCP'] = NA
  wx.proj[wx.proj$STATION %in% bad.precip.stations$STATION,'PRCP'] = NA
  
  # Options: nearest neighbor interpolation, inverse distance weighted, ordinary kriging...
  # Will make one raster for each variable of interest, per day, and then apply to grid/hour.
  # Here use kriging from gstat. Models are all based on spatial variance of the target variabel
  
  # Kriging. Intercept only = ordinary kriging.
  cl <- makeCluster(parallel::detectCores())
  registerDoParallel(cl)
  
  # Create an empty raster grid for the state; we will interpolate over this grid, then assign values from the raster to each grid cell. Increase n for smaller raster cells (more time-intensive)
  # For Bellevue, use the total wx.proj as the surface rather than rd_shp, because the Bellevue road network is a smaller subset of the whole area of King County
  
  grd <- as.data.frame(spsample(wx.proj, 'regular', n = 5000))
  names(grd) <- c("X", "Y")
  coordinates(grd) <- c("X", "Y")
  gridded(grd) <- TRUE # for SpatialPixel
  fullgrid(grd) <- TRUE # for SpatialGrid
  proj4string(grd) <- proj4string(rd_shp)
  
  StartTime <- Sys.time()
  
  writeLines(c(""), paste0("Prep_Weather_Bellevue_log.txt"))    
  
  # Start parallel loop ----
  
  wx.grd.day <- foreach(day = unique(wx$DATE), 
                      .packages = c('raster','gstat','dplyr','rgdal'), 
                      .combine = rbind) %dopar% {
                        
    cat(paste(Sys.time()), as.character(day), "\n", 
        file = paste0("Prep_Weather_Bellevue_log.txt"), append = T) 
        
    # Scan team bucket for completed daily weather prep ----
    fn = paste("Prep_Weather_Daily_", day,"_Bellevue.RData", sep="")
    
    # See if exists in S3. Load if so. If not, carry out kriging steps.
    exists_fn <- length(
      system(paste("aws s3 ls",
                 file.path(teambucket, state, 'Daily_Weather_Prep', fn)), intern= T)) > 0
    
    if(exists_fn){
      system(paste("aws s3 cp",
                   file.path(teambucket, state, 'Daily_Weather_Prep', fn),
                   file.path(temp.outputdir, fn)
      ))
      load(file.path(temp.outputdir, fn))
    } else {
    
    wx.day = wx.proj[wx.proj$DATE == day,]
                        
    f.p <- as.formula(PRCP ~ 1)
    
    vg_prcp <- gstat::variogram(PRCP ~ 1, locations = wx.day[!is.na(wx.day$PRCP),])
    dat.fit <- fit.variogram(vg_prcp, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.prcp <- krige(f.p, wx.day[!is.na(wx.day$PRCP),], grd, dat.fit)
    
    # Rasterize
    prcp_r <- raster::raster(dat.krg.prcp)

    # Do tmin, tmax, and snow
    f.tmin <- as.formula(TMIN ~ 1)
    vg_tmin <- variogram(TMIN ~ 1, wx.day[!is.na(wx.day$TMIN),])
    dat.fit <- fit.variogram(vg_tmin, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.tmin <- krige(f.tmin, wx.day[!is.na(wx.day$TMIN),], grd, dat.fit)
    tmin_r <- raster::raster(dat.krg.tmin)
    
    f.tmax <- as.formula(TMAX ~ 1)
    vg_tmax <- variogram(TMAX ~ 1, wx.day[!is.na(wx.day$TMAX),])
    dat.fit <- fit.variogram(vg_tmax, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.tmax <- krige(f.tmax, wx.day[!is.na(wx.day$TMAX),], grd, dat.fit)
    tmax_r <- raster::raster(dat.krg.tmax)
    
    f.snow <- as.formula(SNOW ~ 1)
    vg_snow <- variogram(SNOW ~ 1, wx.day[!is.na(wx.day$SNOW),])
    dat.fit <- fit.variogram(vg_snow, fit.ranges = F, fit.sills = F,
                             vgm(model = "Sph"))
    dat.krg.snow <- krige(f.snow, wx.day[!is.na(wx.day$SNOW),], grd, dat.fit)
    snow_r <- raster::raster(dat.krg.snow)
    
    # Apply to segments in year-day ----

    # Need to extract values from the raster layers to the lines
    prcp_extr <- raster::extract(x = prcp_r,   # Raster object
                                 y = rd_shp,   # SpatialLines
                                 fun = mean,
                                 df = TRUE)

    names(prcp_extr)[2] = "PRCP"
    prcp_extr$ID = rd_shp$OBJECTID
    
    daily_result <- data.frame(day, prcp_extr)
    
    tmin_extr <- raster::extract(x = tmin_r,   # Raster object
                                 y = rd_shp,   # SpatialLines
                                 fun = mean,
                                 df = TRUE)
    names(tmin_extr)[2] = "TMIN"
    tmin_extr$ID = rd_shp$OBJECTID
    
    daily_result <- full_join(daily_result, tmin_extr)
    
    tmax_extr <- raster::extract(x = tmax_r,   # Raster object
                                 y = rd_shp,   # SpatialLines
                                 fun = mean,
                                 df = TRUE)
    names(tmax_extr)[2] = "TMAX"
    tmax_extr$ID = rd_shp$OBJECTID
    
    daily_result <- full_join(daily_result, tmax_extr)
    
    snow_extr <- raster::extract(x = snow_r,   # Raster object
                                 y = rd_shp,   # SpatialLines
                                 fun = mean,
                                 df = TRUE)
    names(snow_extr)[2] = "SNOW"
    snow_extr$ID = rd_shp$OBJECTID
    
    daily_result <- full_join(daily_result, snow_extr)
    
    # Save to S3 as temporary location in case the process is interrupted
    fn = paste("Prep_Weather_Daily_", day,"_Bellevue.RData", sep="")
    
    save(list="daily_result", file = file.path(temp.outputdir, fn))
    
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, fn),
                 file.path(teambucket, state, 'Daily_Weather_Prep', fn)))
    
    EndTime <- Sys.time()-StartTime
    cat(as.character(day), 'completed', round(EndTime, 2), attr(EndTime, 'units'), '\n',
        file = paste0("Prep_Weather_Bellevue_log.txt"), append = T) 
    
    } # end if exists_fn
    daily_result
  } # end parallel loop
   
  # Plot gridded versions of same point maps to check
  source(file.path(codeloc, 'WA', 'datacleaning', 'Plot_weather_segmented.R'))
  
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


no_prcp = wx.grd.day %>% 
  filter(is.na(PRCP)) %>%
  group_by(day) %>%
  count()

# Export ----
# Export for joining with other road segment data and modeling off SDC
format(object.size(wx.grd.day), 'Mb')
dim(wx.grd.day)


write.csv(wx.grd.day, file = file.path(localdir, 'Weather', 'Prepared_Bellevue_Wx_2018.csv'), row.names = F)

zipname = paste0('Prepared_Bellevue_Wx_2018_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "Weather", paste0(prepname, ".RData")),
             file.path(localdir, 'Weather', 'Prepared_Bellevue_Wx_2018.csv')
             ))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))
