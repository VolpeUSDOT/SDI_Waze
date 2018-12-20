# Prepare special events for random forest work
# Need to apply points to grids

# Run from RandomForest_WazeGrid_TN.R
# already in memory are localdir and g, which repreresents the grid type to use

# append.hex2(hexname = w, data.to.add = "TN_SpecialEvent", state = state, na.action = na.action)
library(rgeos)
library(rgdal)

proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Combine 2017 and 2018 data
load(file.path(localdir, "SpecialEvents", "TN_SpecialEvent_2017.RData")) # 9 columns
spev2017 <- spev # 670 rows
load(file.path(localdir, "SpecialEvents", "TN_SpecialEvent_2018.RData")) # 13 columns
spev2018 <- spev[,names(spev2017)] # 813 rows
spev <- rbind(spev2017, spev2018) # 1483 rows
table(spev$TimeZone) # 819 rows codes with TimeZone information. There are still a lot of 2017 dataset that do not have the TimeZone coded. Jessie will re-do the TimeZone columns locally.

# # If not running from RandomForest_Wazegrid_TN.R, set it up manually by running following code.
# grids = c("TN_01dd_fishnet",
#           "TN_1sqmile_hexagons")
# g = grids[2]
# 
# user <- if(length(grep("@securedatacommons.com", getwd())) > 0) {
#   paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
# } else {
#   paste0( "/home/", system("whoami", intern = TRUE))
# } # find the user directory to use
# 
# localdir <- paste0(user, "/workingdata/TN") # full path for readOGR


# Check to see if these processing steps have been done yet; load from prepared file if so
prepname = paste("Prepared", "TN_SpecialEvent", g, sep="_")

if(length(grep(prepname, dir(file.path(localdir, "SpecialEvents")))) == 0) { # if doen't exist in TN/SpecialEvents, make it
  
  cat("Preparing", "TN_SpecialEvent", g, "\n")
  
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
  
  # Expand grid by hour
  # If there is no start / end time, apply from 0 to 24
  
  # Make framework to join into
  all.hour <- seq(from = as.POSIXct(paste(min(dd$Event_Date), "0:00"), tz = "America/Chicago"), 
                    to = as.POSIXct(paste(max(dd$Event_Date), "0:00"), tz = "America/Chicago"),
                    by = "hour")
  
  GridIDTime <- expand.grid(all.hour, unique(dd$GRID_ID))
  names(GridIDTime) <- c("GridDayHour", "GRID_ID")
  
  # One solution: convert everything to central!
  
  start.hr <- as.numeric(format(strptime(dd$StartTime, format = "%H:%M:%S"), "%H"))
  start.hr[dd$TimeZone == "America/New_York"] = start.hr[dd$TimeZone == "America/New_York"] - 1
  start.hr[is.na(start.hr)] = 0 # start at midnight for all day events
  
  end.hr <- as.numeric(format(strptime(dd$EndTime, format = "%H:%M:%S"), "%H"))
  end.hr[dd$TimeZone == "America/New_York"] = end.hr[dd$TimeZone == "America/New_York"] - 1
  end.hr[is.na(end.hr)] = 23 # end at 11pm hr for all day events
  
  # Attempt at individual event TZ work -- Need to apply timezones individually; vector of mixed time zone values is not accepted by strptime or as.POSIX* functions.
  # start.hr.time <- end.hr.time <- vector()
  
  # for(i in 1:nrow(dd)){
  #   start.hr.time = rbind(start.hr.time, strptime(paste(dd$Event_Date[i], start.hr[i]), "%Y-%m-%d %H", tz = dd$TimeZone[i]))
  #   end.hr.time = c(end.hr.time, strptime(paste(dd$Event_Date[i], end.hr[i]), "%Y-%m-%d %H", tz = dd$TimeZone[i]))
  # }
  
  dd <- data.frame(dd, 
                   start.hr = strptime(paste(dd$Event_Date, start.hr), "%Y-%m-%d %H", tz = "America/Chicago"),
                   end.hr = strptime(paste(dd$Event_Date, end.hr), "%Y-%m-%d %H", tz = "America/Chicago"))
  
  StartTime <- Sys.time()
  t.min = min(GridIDTime$GridDayHour)
  t.max = max(GridIDTime$GridDayHour)
  i = t.min
  
  spev.grid.time.all <- vector()
  counter = 1
  while(i+3600 <= t.max){
    ti.GridIDTime = filter(GridIDTime, GridDayHour == i)
    ti.spev = filter(dd, start.hr >= i & start.hr <= i+3600 | end.hr >= i & end.hr <= i+3600)
    
    ti.spev.hex <- inner_join(ti.GridIDTime, ti.spev, by = "GRID_ID") # Use left_join to get zeros if no match  
    spev.grid.time.all <- rbind(spev.grid.time.all, ti.spev.hex)
    
    i=i+3600
    if(counter %% 3600*24 == 0) cat(paste(i, "\n"))
  } # end loop
  
  EndTime <- Sys.time() - StartTime
  cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
  
  spev.grid.time <- filter(spev.grid.time.all, !is.na(GRID_ID))   
  
  save(list = c("spev.grid.time"), 
       file = file.path(localdir, "SpecialEvents", paste0(prepname, ".RData")))
  
  # Copy to S3
  system(paste("aws s3 cp",
               file.path(localdir, "SpecialEvents", paste0(prepname, ".RData")),
               file.path(teambucket, state, "SpecialEvents", paste0(prepname, ".RData"))))
  
  
  # ~ 3 min  
  } else {
  load(file.path(localdir, "SpecialEvents", paste0(prepname, ".RData")))
}

