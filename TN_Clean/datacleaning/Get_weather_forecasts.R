# Get weather forecasts for state of interest

# Setup ----
inputdir <- file.path(getwd(),"Input")
outputdir<- file.path(getwd(),"Output")

source('utility/get_packages.R')
ON_SDC = F
if(ON_SDC){
  teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
  user <- file.path( "/home", system("whoami", intern = TRUE)) # the user directory 
}

library(ggmap)
library(httr) # for GET
library(tidyverse)
library(xml2) # for xml parsing in tidy way
library(XML) # for xmlToList
library(jsonlite)
library(tigris) # for state shapefiles (or other census geographies)
library(sf)

# Get weather wx_data ----

# The below is a more generalized approach to set the points for which to query weather data 
# (based on state of interest). For TN it comes up with 14 points, whereas the original version for TN had
# 11 points throughout the state. Method seems to work for all states except for Alaska. It does work
# for Hawaii, but only puts 2 points there.
# Eventual TO - DO item - figure out why it doesn't work for Alaska and fix it.

## Setting State here for now, but will likely move to main script/location later
state <- "Tennessee"

# Access state boundary using tigris package, which loads Census TIGER/Line Shapefiles
state_map <- states(cb = TRUE, year = 2021) %>%
  filter_state(state)
# default projection from the above is EPSG 4269 for all states, including Hawaii and Alaska

# Overlay a grid of points within the state - current resolution is 1 degree by 1 degree - 
# This is imperfect because the distances between longitude degrees are greater as one 
# moves closer to the equator and smaller as one moves closer to one of the poles.
# Close enough for our purposes as it puts about the right number of query points in each 
# state.
grd <- state_map %>% 
  st_make_grid(cellsize = c(1,1), what = "centers") %>% 
  st_intersection(state_map) 

# Uncomment the below code to view the grid overlay
# ggplot() +
#   geom_sf(data = state_map, aes(), fill = NA, alpha = 1) +
#   geom_sf(data = grd, aes())

# Create dataframe with the longitude and latitude of the grid points.
queries <- st_coordinates(grd) %>% as.data.frame()

# Using TomorrowIO now for the actual API call (can add NOAA GFS as an option later)
TomorrowIO = T
if(TomorrowIO) {
  # User needs to visit the tomorrow io website to obtain a free account and then put their api key into
  # a text file called "WeatherAPI_key.txt" in the "Weather" folder within the "Input" folder.
  # Next line obtains the user's api key.
  w_key = scan(file.path(inputdir,"Weather", 'WeatherAPI_key.txt'), what = 'character')
  # This is the format needed for an API call in TomorrowIO, with three parameters that
  # need to be filled in (latitude, longitude, and API key):
  # https://api.tomorrow.io/v4/weather/forecast?location={latitude},{longitude}&apikey={key}
  # Next line appends a column to the queries dataframe with the constructed url for each query.
  queries = queries %>% 
    mutate(url = paste0('https://api.tomorrow.io/v4/weather/forecast?location=',Y,',',X,'&apikey=', w_key))
  
  # Daily data are available for 6 days (including the current day, so only 5 future days)
  # Hourly data are available for 120 hours in the future (i.e. 5 future days), so about the 
  # same timeframe. We can discuss whether we want to shift to use hourly data instead of daily.
  # By default, time zone is Coordinated Universal Time (UTC)... the offset will vary by state
  # based on the time zone(s). May also vary based on whether it is daylight savings or standard time?
   
  # TO-DO: figure out what to do for the larger states (limit for TomorrowIO is 25 requests per hour,
  # so grd object created above should not have more than 25 points).
  
  # create blank dataframes to populate with weather data
   
  weather_daily <- data.frame(lon = numeric(),
                              lat = numeric(),
                              date = Date(), 
                              temperatureMin = numeric(),
                              temperatureMax = numeric(),
                              snowAccumulationSum = numeric(),
                              rainAccumulationSum = numeric(),
                              sleetAccumulationLweSum = numeric(),
                              iceAccumulationLweSum = numeric())
  
  weather_hourly <- data.frame(lon = numeric(),
                               lat = numeric(),
                               hour = POSIXct(), 
                               temperature = numeric(),
                               snowAccumulation = numeric(),
                               rainAccumulation = numeric(),
                               sleetAccumulationLwe = numeric(),
                               iceAccumulation = numeric())
  
  # uncomment this for testing/development 
  # i <- 14
  
  # Loop over the json queries for each forecast point and add to the data frames (daily and hourly)
  # Pause for 0.34 seconds in between each query because the limit is 3 requests per second
  # Note there are also limits of 25 requests per hour and 500 requests per day.
   
  for(i in 1:nrow(queries)){
    wx_dat_i = fromJSON(queries$url[i])
    
    # extract the weather attributes for the six days as a dataframe
    wx_dat_daily_values = wx_dat_i$timelines$daily$values %>%
      # extract the dates and assign to a new column
      mutate(date = as.Date(wx_dat_i$timelines$daily$time),
             lon = queries$X[i],
             lat = queries$Y[i]) %>%
      # subset to only retain the necessary columns
      select(lon, lat, date, temperatureMin,temperatureMax,snowAccumulationSum, rainAccumulationSum, sleetAccumulationLweSum,iceAccumulationSum)
    
    # extract the weather attributes for the 120 hours as a dataframe
    wx_dat_hourly_values = wx_dat_i$timelines$hourly$values %>% 
      # extract the hours and assign to a new column
      mutate(hour = ymd_hms(wx_dat_i$timelines$hourly$time),
             lon = queries$X[i],
             lat = queries$Y[i]) %>%
      # subset to only retain the necessary columns
      select(lon, lat, hour,temperature,snowAccumulation, rainAccumulation, sleetAccumulationLwe,iceAccumulation)
    
    # add forecasts for this point to the data frames along with the rest of the points
    weather_daily <- rbind(weather_daily,wx_dat_daily_values)
    weather_hourly <- rbind(weather_hourly,wx_dat_hourly_values)
    
    Sys.sleep(0.34)
  }
}
rm(list=c("wx_dat_daily_values","wx_dat_hourly_values"))
  
  ## TO-DO: need generic method for automatically getting/setting time zone. What to do if state
  ## crosses multiple time zones?
  # By default, TomorrowIO reported time zone is Coordinated Universal Time (UTC)... the offset 
  # to convert will vary by state based on the time zone(s). May also vary based on whether it is 
  # daylight savings or standard time?
  
# Get correct time zone from shapefile
wx_dat$time <- as.POSIXct(wx_dat$time, origin = '1970-01-01', tz = 'America/Chicago')

# Key reference for cross-walking sp to sf commands:
# https://github.com/r-spatial/sf/wiki/migrating
# 
# from: https://stackoverflow.com/questions/73900335/alternatives-to-sptransfrom-function-due-retirement-of-rgdal
# You have to switch to using sf spatial data frames instead of sp spatial data frames 
# in order to use st_transform with sf.
# You construct these with functions like st_as_sf to convert from a data frame or st_read 
# to read from standard data formats like Shapefiles (although use GeoPackage if you can). 
# Tutorials are online.
# 
# The big problem is if you want to call functions in other packages that need sp data frames, 
# and have not been converted to use sf class data frames yet. In this case you should try and 
# keep your workflow using sf for as much as possible, and then convert from one format to the 
# other when necessary for interoperability:
#     
# sfdata = st_as_sf(spdata)
# spdata = as(sfdata, "Spatial")
# 
# See also: https://r-spatial.github.io/sf/reference/st_as_sf.html
# See also: https://r-spatial.github.io/sf/reference/st_transform.html

### TO DO - check with Dan about the below.... this 'proj' object appears to never get used... rationale
### for Albers equal area projection versus the WGS84, versus the original projection identified above?
# Project weather to Albers equal area, ESRI 102008
proj <- showP4(showWKT("+init=epsg:102008"))
 
#  wx_dat.proj <- SpatialPointsDataFrame(coords = wx_dat[c('lon', 'lat')],
#                                    data = wx_dat,
#                                    proj4string = CRS("+proj=longlat +datum=WGS84"))
weather_daily.proj <- st_as_sf(weather_daily,
                               coords = c('lon', 'lat'),
                               crs = 4269)
  
weather_hourly.proj <- st_as_sf(weather_hourly,
                                coords = c('lon', 'lat'),
                                crs = 4269)
# CRS("+proj=longlat +datum=WGS84")
## TO-DO - determine whether we need to come up with a customized projection, or if we can just use 
## epsg 4269 for all of North America. For now, just go with that. Commenting out the below for now.
#    wx_dat.proj <- st_transform(wx_dat.proj, CRS(proj.USGS))  
  
  # Overlay timezone ----
  # Read tz file
  
#  tz <- readOGR(file.path(inputdir,"Shapefiles", 'TimeZone'), layer = 'combined-shapefile')
tz <- read_sf(file.path(inputdir,"Shapefiles", 'TimeZone'), layer = 'combined-shapefile')  
tz <- st_transform(tz, crs = 4269)

# Assign time zone ID ('tzid') to each row for daily and hourly based on intersection with the time zones shapefile, 'tz'
## TO - DO... fix the below... assign the time zones.
## https://stackoverflow.com/questions/74367652/having-trouble-testing-point-geometry-intersections-in-sf

weather_daily.proj$tzid <- st_intersects(weather_daily.proj, tz[,"tzid"])
weather_hourly.proj$tzid <- st_intersects(weather_hourly.proj, tz[,"tzid"])

weather_daily.proj <- st_join(weather_daily.proj, tz[,"tzid"], join = st_intersects)

weather_hourly.proj$local_time <- with_tz(weather_hourly.proj$hour, "GMT")

# ggplot() +
#   geom_sf(data=tz, aes(), fill = NA) +
#   geom_sf(data = state_map, aes(), fill = NA, alpha = 1) +
#   geom_sf(data = weather_daily.proj, aes())+
#   coord_sf(xlim = c(-80, -91), ylim = c(34.9, 36.7), expand = FALSE)+
#   theme_minimal()

  # Loop over every row and apply the correct time zone to the weather forecast times
  local_time = vector()
  for(k in 1:nrow(wx_dat.proj)) {
    local_time = c(local_time, format(as.POSIXct(wx_dat.proj$time[k], origin = '1970-01-01', format = '%F', 
                                     tz = as.character(wx_dat.proj$tzid[k])),
                          "%Y-%m-%d %H:%M:%S", usetz = T))
    
  } # end loop over rows for local_time
  wx_dat <- data.frame(wx_dat, local_time)
  wx_dat.proj@data <- data.frame(wx_dat.proj@data, local_time)
  
  fn = paste0("TN_Forecasts_", Sys.Date(), ".RData")
  
  save(list=c('wx_dat', 'wx_dat.proj'),
       file = file.path(inputdir, "Weather", fn))
  if(ON_SDC){
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(inputdir, "Weather", fn),
                 file.path(teambucket, "TN", "Weather", fn)))
  }
}

if(!DARKSKY){
  # WUNDERGROUND ONLY 
  # Parse XML ----
  
  # Parse to data frame, apply to spatial grid, overlay on crash wx_data for use in models
  # Useful to look at elements of the XML file
  # wx_dat <- xmlParse(get(xmls[1]))
  # wx_dat
  # xmlToList(wx_dat) 
  
  # Within simpleforecast, pick out forecastdays, then each forecastday
  # wx_date: yday, hour
  # high: fahrenheit
  # low: fahrenheit
  # conditions
  # qpf_allday: in
  # snow_allday: in
  # maxwind: mph
  # avewind: mph
  
  wx_dat <- vector()
  
  # Loop over the xml files from this forecast and put into a wx_data frame
  for(i in 1:length(xmls)){
    check <- xmlToList(xmlParse(get(xmls[i])))[1]
    check <- grep("backend failure", check)
    if(length(check) > 0) { cat("XML query", xmls[i], "failed, skipping \n")
      
      } else { 
      
        d1 <- read_xml(get(xmls[i]))
      
        ll <- unlist(strsplit(innames[i], "/")); ll <- ll[length(ll)]
        ll <- unlist(strsplit(ll, ","))
        
        lat <- as.numeric(ll[1])
        lon <- as.numeric(sub("\\.xml", "", ll[2])) 
        yr <- d1 %>% xml_find_all("//wx_date/year") %>% xml_text()
        ydays <- d1 %>% xml_find_all("//wx_date/yday") %>% xml_text()
        th <- d1 %>% xml_find_all("//high/fahrenheit") %>% xml_double()
        tl <- d1 %>% xml_find_all("//low/fahrenheit") %>% xml_double()
        cond <- d1 %>% xml_find_all("//conditions") %>% xml_text()
        qpf <- d1 %>% xml_find_all("//qpf_allday/in") %>% xml_double() # accumulated precip
        snow <- d1 %>% xml_find_all("//snow_allday/in") %>% xml_double()
        maxwind <- d1 %>% xml_find_all("//maxwind/mph") %>% xml_double()
        avewind <- d1 %>% xml_find_all("//avewind/mph") %>% xml_double()
        
        wx_dat <- rbind(wx_dat, wx_data.frame(lat, lon, yr, ydays, th, tl, cond, qpf, snow, maxwind, avewind))
      }
  }

  
  
  
  # Save forecasts ----
  
  save(list='wx_dat',
       file = file.path(inputdir, "Weather", paste0("TN_Forecasts_", Sys.wx_date(), ".Rwx_data")))
  
  forecasts <- dir(file.path(inputdir, "Weather"))[grep("^TN_Forecasts_", dir(file.path(inputdir, "Weather")))]
  
  if(ON_SDC){
    # Copy to S3
    for(f in forecasts){
      system(paste("aws s3 cp",
                   file.path(inputdir, "Weather", f),
                   file.path(teambucket, "TN", "Weather", f))) 
  }
  }
  
  
# Next steps: interpolate to state level using kriging or other methods, see 
# http://rspatial.org/analsis/rst/4-interpolation.html
} # end Weather Underground

