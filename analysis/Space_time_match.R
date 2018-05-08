# Time-space match points
# Matching EDT and Waze events.

# Goal: for a given EDT event, find the matching Waze events within 60 min on either side, within a 0.5 mi radius.
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.

# use spDists from sp package to get distances from each EDT event to each Waze event. 
# Produce a link table which has a two columns: EDT events and the Waze events which match them; repeat EDT event in the column for all matching Waze events.
# modified for multiple states, on SDC

# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(sp)
library(tidyverse)
library(rgdal)
library(maps)

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

localdir <- "/home/daniel/workingdata" # full path for readOGR

wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary
edtdir <- file.path(localdir, "EDT") # Unzip and rename with shortnames if necessary

setwd(localdir)

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Set parameters: states, yearmonths, time zone and projection
states = c("CT", "UT", "VA", "MD")

yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:4, width = 2, flag = "0"), sep="-")
)

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                    "US/Mountain",
                    "US/Eastern",
                    "US/Eastern"),
                  stringsAsFactors = F)

# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# Start loop over states ---

for(i in states) { # i= "UT"
  
  # Start loop over months ----
  wazemonthfiles <- dir(wazedir)[grep(i, dir(wazedir))]
  wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
  
  # get EDT data for this state
  e_i <- read.csv(file.path(edtdir, dir(edtdir)[grep(i, dir(edtdir))]),
                  na.strings = c("NA", "NULL")) 
  
  # find rows with missing lat/long
  # Discard rows with no lat long
  cat(i, "EDT missing lat/long: \n", summary(is.na(e_i$GPSLat)), "\n")
  e_i <- e_i[!is.na(e_i$GPSLat) & !is.na(e_i$GPSLong),]
  cat(i, "EDT lat/long marked 777: \n", summary(e_i$GPSLat > 77 & e_i$GPSLong < -777), "\n")
  e_i <- e_i[!e_i$GPSLat > 77 & !e_i$GPSLong < -777,]
  
  # Make sure just this state represented
  e_i <- e_i[e_i$CrashState == state.name[state.abb == i],]
  
  use.tz <- tzs$tz[tzs$states == i]
  
  e_i$CrashDate_Local <- with(e_i,
                              strptime(
                                paste(substr(CrashDate, 1, 10),
                                      HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = use.tz)
  )
  
  
  edtmonths <- sort(unique(format(e_i$CrashDate_Local, "%Y-%m")))
  wazemonths <- sort(unique(substr(wazemonthfiles, 4, 10)))
  
  months_shared <- edtmonths[edtmonths %in% wazemonths]
  # make sure months are actually shared 
  stopifnot(all(months_shared == wazemonths[wazemonths %in% edtmonths]))
  
  starttime_total <- Sys.time()
  
  for(j in months_shared){ # j = "2018-04"
    starttime_month <- Sys.time()
    
    load(file.path(wazedir, paste0(i, "_", j, ".RData")))
    
    # Make spatial and set projections
    d <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                                proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF
    
    d <- spTransform(d, CRS(proj))
    
    if(class(e_i)=="data.frame"){
    e_i <- SpatialPointsDataFrame(e_i[c("GPSLong", "GPSLat")], e_i, 
                                proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure EDT data is a SPDF
    
    e_i <-spTransform(e_i, CRS(proj))
    }
    
    # Check with a plot:
    plot(e_i, main = paste("EDT Waze overlay \n", i, j))
    plot(d, add = T, col = "red")
    dev.print(jpeg, file = paste0("Figures/Link_plot_", i, "_" , j, ".jpeg"), width= 500, height = 500)
    
    # <><><><><><><><><><><><><><><><><><><><><><><><>
    link.all <- makelink(accfile = e_i, 
                         incfile = d,
                         acctimevar = "CrashDate_Local",
                         inctimevar1 = "time",
                         inctimevar2 = "last.pull.time",
                         accidvar = "ID",
                         incidvar = "alert_uuid")
    # <><><><><><><><><><><><><><><><><><><><><><><><>
    
    write.csv(link.all, file.path(localdir, "Link", paste0("EDT_Waze_link_", j, "_", i, ".csv")), row.names = F)
    
    timediff <- round(Sys.time()-starttime_month, 2)
    cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
  
   
  } # end month loop
  
  timediff.total <- round(Sys.time()-starttime_total, 2)
  cat(timediff.total, attr(timediff.total, "units"), "elapsed in total for", i, "\n", rep("<>", 20), "\n")
  
  
} # end state loop

