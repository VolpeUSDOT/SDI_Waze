# Prepare special events for random forest work
# Need to apply points to grids

# Run from RandomForest_WazeGrid_TN.R
# append.hex2(hexname = w, data.to.add = "TN_SpecialEvent", state = state, na.action = na.action)
library(rgeos)
library(rgdal)

proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"


if(length(grep("TN_SpecialEvent", data.to.add)) > 0){
  
  assign(data.to.add, load(file.path(localdir, "SpecialEvents", "TN_SpecialEvent_2018.RData")),
         envir = globalenv())
  
  # Check to see if these processing steps have been done yet; don't need to re-do for each month.
  prepname = paste("Prepared", data.to.add, g, sep="_")
  
  if(!exists(prepname)) {
    
    cat("Preparing", data.to.add, g, "\n")
    
    dd <- spev %>% filter(!is.na(Lon) | !is.na(Lat))
    
    # Apply to grid
    grid_shp <- rgdal::readOGR(file.path(localdir, "Shapefiles"), layer = g)
    grid_shp <- spTransform(grid_shp, CRS(proj.USGS))
    
    # Project special events
    dd <- SpatialPointsDataFrame(dd[c("Lon", "Lat")], dd, 
                                       proj4string = CRS("+proj=longlat +datum=WGS84"))  
    
    dd <- spTransform(dd, CRS(proj.USGS))
    
    grid_dd_pip <- over(dd, grid_shp[,"GRID_ID"]) # Match a grid ID to events
    
    dd@data <- data.frame(dd@data, grid_dd_pip)
    
    dd <- dd@data
    # Add year, day of year
    dd$Year = format(dd$Event_Date, "%Y")
    dd$day = format(dd$Event_Date, "%j")    
    
    # TODO: Expand grid by hour
       
     
  #   dd <- dd[!duplicated(with(dd, paste(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN))),]
  #   
  #   dd.summax <- dd %>%
  #     group_by(GRID_ID, month, dayofweek, hour) %>%
  #     select(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN, SUM_MAX_AADT_VN) %>%
  #     tidyr::spread(key = F_SYSTEM_VN, value = SUM_MAX_AADT_VN, fill = 0, sep = "_")
  #   
  #   dd.hourmax <- dd %>%
  #     group_by(GRID_ID, month, dayofweek, hour) %>%
  #     select(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN, HOURLY_MAX_AADT) %>%
  #     tidyr::spread(key = F_SYSTEM_VN, value = HOURLY_MAX_AADT, fill = 0, sep = "_")
  #   
  #   # Rename columns and join
  #   dd.summax <- dd.summax %>% 
  #     rename(SUM_MAX_AADT_1 = F_SYSTEM_VN_1,
  #            SUM_MAX_AADT_2 = F_SYSTEM_VN_2,
  #            SUM_MAX_AADT_3 = F_SYSTEM_VN_3,
  #            SUM_MAX_AADT_4 = F_SYSTEM_VN_4,
  #            SUM_MAX_AADT_5 = F_SYSTEM_VN_5)
  #   
  #   dd.hourmax <- dd.hourmax %>% 
  #     rename(HOURLY_MAX_AADT_1 = F_SYSTEM_VN_1,
  #            HOURLY_MAX_AADT_2 = F_SYSTEM_VN_2,
  #            HOURLY_MAX_AADT_3 = F_SYSTEM_VN_3,
  #            HOURLY_MAX_AADT_4 = F_SYSTEM_VN_4,
  #            HOURLY_MAX_AADT_5 = F_SYSTEM_VN_5)
  #   
  #   dd <- full_join(dd.summax, dd.hourmax, by = c("GRID_ID", "month", "dayofweek", "hour"))
  #   
  #   dd$vmt_time = with(dd, paste(month, dayofweek, hour, sep="_"))
  #   
  #   # Save this to global environment for other months to use
  #   assign(prepname, dd, envir = globalenv())
  #   
  # } else {
  #   
  #   # Create vectors in w for month of year, day of week, hour of day in w. This is used for joining on the grid ID and time factors
  #   
  #   
  #   dd = get(prepname, envir = globalenv()) # Use the already prepared data if present in the working enviroment
  #   
  # }
  # 
  # # Extract year from file name
  # yr = substr(hexname, 3, 6)
  # date = strptime(paste(yr, w$day, sep = "-"), "%Y-%j")
  # mo = as.numeric(format(date, "%m"))
  # dow = lubridate::wday(date) # 7  = saturday, 1 = sunday.
  # w$vmt_time = paste(mo, dow, w$hour, sep="_")
  
}

