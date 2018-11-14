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
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(sp)
library(tidyverse)
library(rgdal)
library(maps)

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use

localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary

setwd(localdir) 

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Set parameters: states, yearmonths, time zone and projection
state = "TN"
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:3, width = 2, flag = "0"), sep="-")
)

# Time zone 
tz = "US/Central"

# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# Start loop over months ----

# Grab from S3 if necessary
if(length(grep("TN", dir(wazedir)))==0){
  for(i in yearmonths){
  system(paste("aws s3 cp",
               file.path(teambucket, 'TN', paste0('TN_', i, '.RData')),
               file.path('~', 'tempout', paste0('TN_', i, '.RData'))))
    }
  
}
  
  
wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
wazemonthfiles = wazemonthfiles[-omit]

# get TN crash data 
load("Crash/TN_Crash_Simple_2008-2018.RData")

# find rows with missing lat/long
# Discard rows with no lat long
cat(state, "TN missing lat/long, FALSE TRUE: \n", summary(is.na(tn_crash$LatDecimalNmb)), "\n")
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
  
  tn_crash <- spTransform(tn_crash, CRS(proj))
  }
  
  # Check with a plot:
  tn_crash@data$ym = format(tn_crash$date, '%Y-%m')
  tn_j <- tn_crash[tn_crash@data$ym == j,]
    
  plot(tn_j, main = paste("TN Waze overlay \n", state, j), col = alpha("black", 0.25),
       cex = 0.8)
  d_crash = d[d$alert_type == "ACCIDENT",]
  plot(d_crash, add = T, col = alpha("firebrick", 0.2), cex = 0.8)
  dev.print(jpeg, file = paste0("Figures/Link_plot_", state, "_" , j, ".jpeg"), width= 500, height = 500)
  
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  link.all <- makelink(accfile = tn_crash, 
                       incfile = d,
                       acctimevar = "date",
                       inctimevar1 = "time",
                       inctimevar2 = "last.pull.time",
                       accidvar = "MstrRecNbrTxt",
                       incidvar = "alert_uuid")
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  
  write.csv(link.all, file.path(localdir, "Link", paste0("TN_Waze_link_", j, "_", state, ".csv")), row.names = F)
  
  timediff <- round(Sys.time()-starttime_month, 2)
  cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")

 
} # end month loop

timediff.total <- round(Sys.time()-starttime_total, 2)
cat(timediff.total, attr(timediff.total, "units"), "elapsed in total for", state, "\n")

