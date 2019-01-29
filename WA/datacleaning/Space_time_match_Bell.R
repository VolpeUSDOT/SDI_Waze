# Step 2 of the data cleaning pipeline, follows ReduceWaze_SDC.R
# Time-space match points
# Matching Tennessee crashes and Waze events.

# Goal: for a given crash in the historical TN data, find the matching Waze events within 60 min on either side, within a 0.5 mi radius.
# Location in TN is in LatDecimalNmb and LongDecimalNmb, time is in date field.
# Location in Waze is in lon and lat, time is in time.

# use spDists from sp package to get distances from each TN crash to each Waze event. 
# Produce a link table which has a two columns: TN crashes and the Waze events which match them; repeat TN crash in the column for all matching Waze events.
# modified for multiple states, on SDC

# Setup ----
rm(list = ls())
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))
source(file.path(codeloc, 'utility/Workstation_setup.R')) # Download necessary files from S3

library(sp)
library(tidyverse)
library(rgdal)
library(maps)

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

home.loc <- getwd()
  
user <- if(length(grep("@securedatacommons.com", home.loc)) > 0) {
  paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
  } else {
    paste0( "/home/", system("whoami", intern = TRUE))
  } # find the user directory to use

state = "TN"

localdir <- file.path(user, "workingdata", state) # full path for readOGR

wazedir <- file.path(localdir, 'Waze') # has State_Year-mo.RData files. Grab from S3 if necessary

setwd(localdir) 

# read functions
source(file.path(codeloc, 'TN/utility/wazefunctions_TN.R'))

# Set parameters: states, yearmonths, time zone and projection
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:9, width = 2, flag = "0"), sep="-")
)

# Time zone - now using the tz in each row
# tz = "US/Central"

# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# Start loop over months ----

# # Grab from S3 (save to tempout) if necessary. if have downloaded from S3, this step can ignore
# if(length(grep(state, dir(wazedir)))==0){
#   for(i in yearmonths){
#   system(paste("aws s3 cp",
#                file.path(teambucket, 'TN', paste0('TN_', i, '.RData')),
#                file.path('~', 'tempout', paste0('TN_', i, '.RData'))))
#     }
#   
# }
  
wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
if(length(omit) > 0) { wazemonthfiles = wazemonthfiles[-omit] }

# get TN crash data 
load("Crash/TN_Crash_Simple_2008-2018.RData")

# find rows with missing lat/long
# Discard rows with no lat long
tn_crash <- crash
cat(state, "TN missing lat/long, FALSE: \n", summary(is.na(tn_crash$LatDecimalNmb)), "\n")
tn_crash <- tn_crash[!is.na(tn_crash$LatDecimalNmb) & !is.na(tn_crash$LongDecimalNmb),]

tnmonths <- sort(unique(format(tn_crash$date, "%Y-%m")))
wazemonths <- sort(unique(substr(wazemonthfiles, 4, 10)))

months_shared <- tnmonths[tnmonths %in% wazemonths]
# make sure months are actually shared 
stopifnot(all(months_shared == wazemonths[wazemonths %in% tnmonths]))

starttime_total <- Sys.time()

for(j in months_shared){ # j = "2018-03"
  starttime_month <- Sys.time()
  
  load(file.path(wazedir, paste0(state, "_", j, ".RData")))
  
  # Make spatial and set projections
  d <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF
  
  d <- spTransform(d, CRS(proj))
  
  if(class(tn_crash)=="data.frame"){
  tn_crash <- SpatialPointsDataFrame(tn_crash[c("LongDecimalNmb", "LatDecimalNmb")], tn_crash, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure TN data is a SPDF
  
  tn_crash <- spTransform(tn_crash, CRS(proj)) # Project the data
  }
  
  # Check with a plot:
  tn_crash@data$ym = format(tn_crash$date, '%Y-%m')
  tn_j <- tn_crash[tn_crash@data$ym == j,]
    
  plot(tn_j, main = paste("TN Waze overlay \n", state, j), col = alpha("black", 0.25),
       cex = 0.8)
  d_crash = d[d$alert_type == "ACCIDENT",]
  plot(d_crash, add = T, col = alpha("firebrick", 0.2), cex = 0.8)
  dev.print(jpeg, file = paste0("Figures/Link_plot_", state, "_" , j, ".jpeg"), width= 500, height = 500)
  
  # Get time in the right format if not already
  if(!("POSIXct" %in% class(d$time))){  # class(d$time) has two elements
    dt <- as.character(d$time)
    dt.last <- as.character(d$last.pull.time)
    dtz <- substr(dt, 21, 23) # Get the 3 timezone letters
    
    new.d <- vector()
    
    for(tz in unique(dtz)){ # tz = "CST"
      dx.tz = d[dtz == tz,] # subset data frame d to just rows with this time zone
      dx.tz$time = as.POSIXct(dt[dtz == tz], tz = tz)
      dx.tz$last.pull.time = as.POSIXct(dt.last[dtz == tz], tz = tz)
      new.d = rbind(new.d, dx.tz@data)
    }
    d = new.d; rm(new.d, dt, dt.last, dtz)  
    d <- SpatialPointsDataFrame(d[c("lon", "lat")], d, 
                                proj4string = CRS("+proj=longlat +datum=WGS84")) # convert back to spatial points data frame
    
    d <- spTransform(d, CRS(proj))
    }
  
  
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  link.all <- makelink(accfile = tn_j, # just this month subset 
                       incfile = d,
                       acctimevar = "date",
                       inctimevar1 = "time",
                       inctimevar2 = "last.pull.time",
                       accidvar = "MstrRecNbrTxt",
                       incidvar = "alert_uuid")
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  # # check how many are matched
  # length(unique(link.all$id.accident))/nrow(tn_j@data) # 2562/6894  for "2017-04"
  # length(unique(link.all$id.incidents))/nrow(d@data) # 13231/281937   for "2017-04"
  # table(tn_j@data$hour[!tn_j@data$MstrRecNbrTxt %in% unique(link.all$id.accident)]) # still most of the unmatched TN crashes happened in day time, "2017-04"
  # # 00  01  02  03  04  05  06  07  08  09  10  11  12  13  14  15  16  17  18  19  20  21  22  23
  # # 371  54  51  44  40  69 105 208 136 139 161 234 249 268 294 358 310 286 203 159 184 150 134 125
  
  # Save linked data
  write.csv(link.all, file.path(localdir, "Link", paste0("TN_Waze_link_", j, "_", state, ".csv")), row.names = F)
  
  timediff <- round(Sys.time()-starttime_month, 2)
  cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")

 
} # end month loop

timediff.total <- round(Sys.time()-starttime_total, 2)
cat(timediff.total, attr(timediff.total, "units"), "elapsed in total for", state, "\n")

