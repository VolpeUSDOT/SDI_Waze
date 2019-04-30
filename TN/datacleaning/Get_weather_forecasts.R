# Get Weather forecasts for Tennessee, using Weatherunderground API


# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

library(ggmap)
library(httr) # for GET
library(tidyverse)
library(xml2) # for xml parsing in tidy way
library(XML) # for xmlToList
library(jsonlite)

user <- file.path( "/home", system("whoami", intern = TRUE)) # the user directory 
localdir <- file.path(user, "workingdata", "TN") # full path for readOGR

setwd(localdir)

proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Get weather data ----

# Provided by TN: WeatherUnderground_171228.txt. This is a small script to be run in a Windows enviroment, to get XML-format files for 11 locations in TN. Here we'll read in the file as is, just to extract the API addresses
# WUndground *no longer is providing free API keys*, so we will see if the API key in these addresses will work. Free version works through end of 2018.

wu <- scan(file = "Weather/WeatherUnderground_171228.txt",
           what = 'character')


xmlnames <- wu[grep(".xml$", wu)]

outnames <- xmlnames[nchar(xmlnames) < 15]
innames <- xmlnames[nchar(xmlnames) > 15]

# Using DarkSky now
DARKSKY = T
if(DARKSKY) {
  
  ds_key = scan(file.path(localdir, 'DarkSky_Key.txt'), what = 'character')
  ds_ll_query = ll = vector()
  
  for(i in innames){
    # extract just the lat long
    i_ll = strsplit(i, '/') 
    i_ll = i_ll[[1]][[length(i_ll[[1]])]]
    i_ll = sub('.xml', '', i_ll)  
    ds_ll_query = c(ds_ll_query,
                    paste('https://api.darksky.net/forecast', ds_key,  i_ll, sep = '/'))
    ll = c(ll, i_ll)
  }
  innames = ds_ll_query
  
  # Parse JSON
  
  dat <- vector()
  
  # Loop over the json queries from this forecast and put into a data frame
  for(i in 1:length(innames)){
    
    ll_i = strsplit(ll[i], ",")[[1]]
    
    dat_i = fromJSON(innames[i])$hourly$data
    
    dat_i = data.frame(lat = ll_i[1], lon = ll_i[2], dat_i)
    dat = rbind(dat, dat_i)
    
  }

  # Get correct time zone from shapefile
 # dat$time <- as.POSIXct(dat$time, origin = '1970-01-01', tz = 'America/Chicago')
  dat$lat = as.numeric(as.character(dat$lat))
  dat$lon = as.numeric(as.character(dat$lon))
  
  # Project weather to Albers equal area, ESRI 102008
  proj <- showP4(showWKT("+init=epsg:102008"))
  
  dat.proj <- SpatialPointsDataFrame(coords = dat[c('lon', 'lat')],
                                    data = dat,
                                    proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  dat.proj <- spTransform(dat.proj, CRS(proj.USGS))
  
  # Overlay timezone 
  # Read tz file
  tz <- readOGR(file.path(localdir, 'dist'), layer = 'combined-shapefile')
  
  
  tz <- spTransform(tz, CRS(proj.USGS))
  
  wx_tz <- over(dat.proj, tz[,"tzid"]) # Match a tzid name to each row in dat.proj weather data 
  
  dat.proj@data <- data.frame(dat.proj@data, tzid = as.character(wx_tz$tzid))
  
  # Loop over every row and apply the correct time zone to the weather forecast times
  local_time = vector()
  for(k in 1:nrow(dat.proj)) {
    local_time = c(local_time, format(as.POSIXct(dat.proj$time[k]/1000, origin = "1970-01-01", 
                                     tz = as.character(dat.proj$tzid[k])),
                          "%Y-%m-%d %H:%M:%S", usetz = T))
    
  } # end loop over rows for local_time
  

  save(list=c('dat', 'dat.proj'),
       file = file.path(localdir, "Weather", paste0("TN_Forecasts_", Sys.Date(), ".RData")))
  
  forecasts <- dir(file.path(localdir, "Weather"))[grep("^TN_Forecasts_", dir(file.path(localdir, "Weather")))]
  
  # Copy to S3
  for(f in forecasts){
    system(paste("aws s3 cp",
                 file.path(localdir, "Weather", f),
                 file.path(teambucket, "TN", "Weather", f)
    ))
  }
  
}





if(!DARKSKY){
  # WUNDERGROUND ONLY 
  # Parse XML ----
  
  # Parse to data frame, apply to spatial grid, overlay on crash data for use in models
  # Useful to look at elements of the XML file
  # dat <- xmlParse(get(xmls[1]))
  # dat
  # xmlToList(dat) 
  
  # Within simpleforecast, pick out forecastdays, then each forecastday
  # date: yday, hour
  # high: fahrenheit
  # low: fahrenheit
  # conditions
  # qpf_allday: in
  # snow_allday: in
  # maxwind: mph
  # avewind: mph
  
  dat <- vector()
  
  # Loop over the xml files from this forecast and put into a data frame
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
        yr <- d1 %>% xml_find_all("//date/year") %>% xml_text()
        ydays <- d1 %>% xml_find_all("//date/yday") %>% xml_text()
        th <- d1 %>% xml_find_all("//high/fahrenheit") %>% xml_double()
        tl <- d1 %>% xml_find_all("//low/fahrenheit") %>% xml_double()
        cond <- d1 %>% xml_find_all("//conditions") %>% xml_text()
        qpf <- d1 %>% xml_find_all("//qpf_allday/in") %>% xml_double() # accumulated precip
        snow <- d1 %>% xml_find_all("//snow_allday/in") %>% xml_double()
        maxwind <- d1 %>% xml_find_all("//maxwind/mph") %>% xml_double()
        avewind <- d1 %>% xml_find_all("//avewind/mph") %>% xml_double()
        
        dat <- rbind(dat, data.frame(lat, lon, yr, ydays, th, tl, cond, qpf, snow, maxwind, avewind))
      }
  }

  
  
  
  # Save forecasts ----
  
  save(list='dat',
       file = file.path(localdir, "Weather", paste0("TN_Forecasts_", Sys.Date(), ".RData")))
  
  forecasts <- dir(file.path(localdir, "Weather"))[grep("^TN_Forecasts_", dir(file.path(localdir, "Weather")))]
  
  # Copy to S3
  for(f in forecasts){
    system(paste("aws s3 cp",
                 file.path(localdir, "Weather", f),
                 file.path(teambucket, "TN", "Weather", f)
    ))
  }
  
  
# Next steps: interpolate to state level using kriging or other methods, see 
# http://rspatial.org/analsis/rst/4-interpolation.html
} # end Weather Underground

