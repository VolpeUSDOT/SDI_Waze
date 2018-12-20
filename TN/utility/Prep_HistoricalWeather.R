# Prepare historical weather for random forest work

# Run from RandomForest_WazeGrid_TN.R
# already in memory are localdir and g, which repreresents the grid type to use
# Next step after this script is to run append.hex function to add 

proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Check to see if these processing steps have been done yet; load from prepared file if so
prepname = paste("Prepared", "Weather", g, sep="_")

if(length(grep(prepname, dir(file.path(localdir, "Weather")))) == 0) { 
  
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
    }
  }

  station_file <- file.path(localdir, "Weather", "GHCN", wx.files[grep('stations', wx.files)])
  stations <- read_fwf(station_file,
                       col_positions = fwf_empty(station_file))
  names(stations) = c("STATION", "lat", "lon", "masl", "NAME", "x1", "x2", "x3")
  
  wx$DATE <- as.Date(as.character(wx$DATE))
  
  wx <- wx[order(wx$DATE, wx$STATION),]
  
  # To match with forecast, want the following by day
  # date: yday, hour
  # high: fahrenheit
  # low: fahrenheit
  # conditions
  # qpf_allday: in
  # snow_allday: in
  # maxwind: mph
  # avewind: mph

  wx <- left_join(wx, stations[1:5], by = "STATION")
  
  
  # Next steps: 
  # interpolate to state level using kriging or other methods, see 
  # http://rspatial.org/analsis/rst/4-interpolation.html
  # then apply to grid cells in year-day 
  
  # Apply to grid
  grid_shp <- rgdal::readOGR(file.path(localdir, "Shapefiles"), layer = g)
  grid_shp <- spTransform(grid_shp, CRS(proj.USGS))
  
  # Project special events
  wx.p <- SpatialPointsDataFrame(wx[c("lon", "lat")], wx, 
                                     proj4string = CRS("+proj=longlat +datum=WGS84"))  
  
  wx.proj <- spTransform(wx.p, CRS(proj.USGS))
  

  #   grid_dd_pip <- over(dd, grid_shp[,"GRID_ID"]) # Match a grid ID to events
  # 
  # dd@data <- data.frame(dd@data, grid_dd_pip)
  # 
  # dd <- dd@data
  # # Add year, day of year
  # dd$Year = format(dd$Event_Date, "%Y")
  # dd$day = format(dd$Event_Date, "%j")    
  # 
  # # Expand grid by hour
  # # If there is no start / end time, apply from 0 to 24
  # 
  # # Make framework to join into
  # all.hour <- seq(from = as.POSIXct(paste(min(dd$Event_Date), "0:00"), tz = "America/Chicago"), 
  #                   to = as.POSIXct(paste(max(dd$Event_Date), "0:00"), tz = "America/Chicago"),
  #                   by = "hour")
  # 
  # GridIDTime <- expand.grid(all.hour, unique(dd$GRID_ID))
  # names(GridIDTime) <- c("GridDayHour", "GRID_ID")
  # 
  # # One solution: convert everything to central!
  # 
  # start.hr <- as.numeric(format(strptime(dd$StartTime, format = "%H:%M:%S"), "%H"))
  # start.hr[dd$TimeZone == "America/New_York"] = start.hr[dd$TimeZone == "America/New_York"] - 1
  # start.hr[is.na(start.hr)] = 0 # start at midnight for all day events
  # 
  # end.hr <- as.numeric(format(strptime(dd$EndTime, format = "%H:%M:%S"), "%H"))
  # end.hr[dd$TimeZone == "America/New_York"] = end.hr[dd$TimeZone == "America/New_York"] - 1
  # end.hr[is.na(end.hr)] = 23 # end at 11pm hr for all day events
  # 
  # dd <- data.frame(dd, 
  #                  start.hr = strptime(paste(dd$Event_Date, start.hr), "%Y-%m-%d %H", tz = "America/Chicago"),
  #                  end.hr = strptime(paste(dd$Event_Date, end.hr), "%Y-%m-%d %H", tz = "America/Chicago"))
  # 
  # StartTime <- Sys.time()
  # t.min = min(GridIDTime$GridDayHour)
  # t.max = max(GridIDTime$GridDayHour)
  # i = t.min
  # 
  # weather.grid.time.all <- vector()
  # counter = 1
  # while(i+3600 <= t.max){
  #   ti.GridIDTime = filter(GridIDTime, GridDayHour == i)
  #   ti.weather = filter(dd, start.hr >= i & start.hr <= i+3600 | end.hr >= i & end.hr <= i+3600)
  #   
  #   ti.weather.hex <- inner_join(ti.GridIDTime, ti.weather, by = "GRID_ID") # Use left_join to get zeros if no match  
  #   weather.grid.time.all <- rbind(weather.grid.time.all, ti.weather.hex)
  #   
  #   i=i+3600
  #   if(counter %% 3600*24 == 0) cat(paste(i, "\n"))
  # } # end loop
  # 
  # EndTime <- Sys.time() - StartTime
  # cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
  # 
  # weather.grid.time <- filter(weather.grid.time.all, !is.na(GRID_ID))   
  # 
  # save(list = c("weather.grid.time"), 
  #      file = file.path(localdir, "Weather", paste0(prepname, ".RData")))
  # 
  # # Copy to S3
  # system(paste("aws s3 cp",
  #              file.path(localdir, "Weather", paste0(prepname, ".RData")),
  #              file.path(teambucket, state, "Weather", paste0(prepname, ".RData"))))
  # 
  # 
  # # ~ 3 min  
  # } else {
  # load(file.path(localdir, "Weather", paste0(prepname, ".RData")))
}

