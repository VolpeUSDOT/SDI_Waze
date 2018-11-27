
##### PURPOSE:  Get special events time and spacial distribution in TN ########
###############################################################################


# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR
wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary

# Install libraries 
library(dplyr)
library(RPostgres)
library(getPass)
library(lubridate)
library(data.table)
library(sp)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(readxl)
library(geonames) # for timezone, see notes below
library(rgeos)
library(tidyverse)


# turn off scienctific notation 
options(scipen = 999)



# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))


# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))


# uncomment these lines and run with user redshift credentials filled in to resolve error if above line throws one.
#Sys.setenv('sdc_waze_username' = 'mgilmore') 
#Sys.setenv('sdc_waze_password' = 'r7ZcGYzjgmu4zxKR')


# Set up workstation to get hexagon shapefiles 
source(file.path(codeloc, 'utility/Workstation_setup.R'))


# Read special event data and convert it to spatial data format
SpecialEvents <- read_excel(file.path(localdir, "TN", "SpecialEvents", "EVENTS Updated 03_21_2017.xls"))


#### Get Waze data 

# Event state
state = "TN"

wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]

# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
wazemonthfiles = wazemonthfiles[-omit]


#### Get Special Event Days and Non-Event Days 

specialevent_type <- "Special" 

se.use = SpecialEvents %>% 
  filter(Event_Type == specialevent_type) 

se.use$Event_Date <- as.Date(as.character(se.use$Event_Date), "%Y-%m-%d")

se.use$StartTime <- as.POSIXct(strptime(se.use$StartTime, format = "%Y-%m-%d %H:%M:%S"))
se.use$StartTime <- format(se.use$StartTime, "%H:%M:%S")

se.use$EndTime <- as.POSIXct(strptime(se.use$EndTime, format = "%Y-%m-%d %H:%M:%S"))
se.use$EndTime <- format(se.use$EndTime, "%H:%M:%S")


# Get timezone of event - TN has 2 timezones (eastern and central) - use geonames package 
# NOTE: only need to run the next two lines once at the initial setup.  
options(geonamesUsername = "waze_sdi") # set this up as a generic username, only need to run this command once 
source(system.file("tests", "testing.R", package = "geonames"), echo = TRUE)

se.use$TimeZone <- 'X'

for (i in 1:nrow(se.use)) {

    se.use$TimeZone[i] <- as.character(GNtimezone(se.use$Lat[i], se.use$Lon[i], radius = 0)$timezoneId)

  }


se.use$StartUTC <- paste(se.use$Event_Date, se.use$StartTime)
se.use$StartUTC <- as.POSIXct(se.use$StartUTC, 
                                   tz = se.use$TimeZone, 
                                   format = "%Y-%m-%d %H:%M:%S")




se.use$EndDateTime <- paste(se.use$Event_Date, se.use$EndTime)
se.use$EndDateTime <- as.POSIXct(se.use$EndDateTime, format = "%Y-%m-%d %H:%M:%S")


# Add comparison non-event days

non.event.df = vector()

for(i in 1:nrow(se.use)){ # i = 1
  non.event.date = se.use$Event_Date[i] + 7
  
  # Test to make sure we get a day non in the special events data or otherise in non-event data
  if(length(non.event.df)==0){  
    while(non.event.date %in% se.use$Event_Date) {
      non.event.date = non.event.date + 7
    }  
  } else {
    while(non.event.date %in% se.use$Event_Date | non.event.date %in% non.event.df$Date) {
      non.event.date = non.event.date + 7
    }  
  }  
  
  non.event.row = se.use[i,]
  non.event.row$Event_Date = non.event.date
  non.event.row$Event_Type = "NonEvent"
  #non.event.row[c("StartTime","EndTime")] = NA
  non.event.df= rbind(non.event.df, non.event.row)
}


# Compile special event and non events together
se.use <- rbind(se.use, non.event.df)
se.use$Event_DateMonth = format(se.use$Event_Date, "%Y-%m")

# format time variables
se.use$DayOfYear <- as.numeric(format(se.use$Event_Date, format = "%j"))
se.use$DayOfWeekNum <- as.numeric(format(se.use$Event_Date, format = "%u"))
se.use$DayofWeek <- format(se.use$Event_Date, format = "%A")


# Empty objects to store output, will rbind at the end of loop
waze.ll = waze.ll.proj = edt.ll = edt.ll.proj = vector()

for(i in 1:nrow(se.use)){ # Start event day loop; i = 1
  eventmo = se.use$Event_DateMonth[i]
  
  specialeventday = se.use$Event_Date[i]
  
  if(length(wazemonthfiles[grep(eventmo, wazemonthfiles)]) > 0){
    
    load(file.path(wazedir, wazemonthfiles[grep(eventmo, wazemonthfiles)])) # load dataframe mb
    
    # Make both Waze and EDT spatial data frame, only picking out relevant columns. w and e are SPDF of Waze and EDT data, for this month
    w <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                                proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF
    
    w <- spTransform(w, CRS(proj))
    # Subset to area of interest only ---- 
    zoomll = se.use %>% dplyr::select(Lon, Lat)
    zoomll = zoomll[!duplicated(zoomll),]
    
    zoomll = SpatialPointsDataFrame(zoomll[c("Lon", "Lat")], zoomll, 
                                    proj4string = CRS("+proj=longlat +datum=WGS84"))
    
    zoomll <- spTransform(zoomll, CRS(proj))
    
    zoom_box = gBuffer(zoomll, width = 4827) # 3 mile circle around FedEx field
    
    zoom_box.ll = spTransform(zoom_box, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
    
    # Step through Waze data to clip to special event buffer and day
    ws = gIntersects(zoom_box, w, byid = T)
    ws = w[ws[,1] == TRUE,] # Make ws as a spatial points data frame of Waze events, for points intersecting the buffer, in this month
    ws.ll = spTransform(ws, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
    
    # Get data frames for plotting. wp and ep are data.frames of ws and es, with projected coordinates
    wp <- data.frame(ws.ll@data, lat = coordinates(ws)[,2], lon = coordinates(ws)[,1])
    
    # Now subset to just the day of interest
    selector = format(wp$time, "%Y-%m-%d") == specialeventday
    
    ll <- data.frame(lat = coordinates(ws.ll)[,2][selector], lon = coordinates(ws.ll)[,1][selector], alert_type = ws.ll$alert_type[selector], time = ws.ll$time[selector], roadclass = ws.ll$road_type[selector])
    
    ll.proj <- data.frame(lat = coordinates(ws)[,2][selector], lon = coordinates(ws)[,1][selector], alert_type = ws$alert_type[selector], time = ws$time[selector], roadclass = ws$road_type[selector])
    
    # Compile
    waze.ll = rbind(waze.ll, ll)
    waze.ll.proj = rbind(waze.ll.proj, ll.proj)
    
    
  }
  
} # End special events day loop





