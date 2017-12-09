# Time-space match points
# Test of matching EDT and Waze events, first using 5 mi square data from Maryland, April 2017.

# Setup ----
library(sp)
library(tidyverse)
library(maps)

# Code location
codeloc <- "~/Documents/git/SDI_Waze"

# load data
source(file.path(codeloc, 'wazeloader.R'))

# read functions
source(file.path(codeloc, 'wazefunctions.R'))

# Set projections
d <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb)  # from monthbind of Waze data, make sure it is a SPDF
edt <- edt.april

proj4string(d) <- proj4string(edt) <- c("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")

# Goal: for a given EDT event, find the matching Waze events within 60 min on either side, within a 0.5 mi radius.
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.

# use spDists from sp package to get distances from each EDT event to each Waze event. Could pre-filter Waze events by lat long; a degree latitude is ~ 86 mi and degree longitude ~ 111 mi for Maryland. If it is too slow, try this.

# Produce a link table which has a two columns: EDT events and the Waze events which match them; repeat EDT event in the column for all matching Waze events.

# link.test <- makelink(edt[1:250,], d[1:5000,])

link.all <- makelink(edt, d)


# Next up: making summary tables from the link table

# EDT files with matching Waze events:
length(unique(fivemi.link$id.edt))
# out of
nrow(edt)
# and with this distribution of Waze matches
hist(tapply(fivemi.link$uuid.waze, fivemi.link$id.edt, length))


# Now try with a larger data set:



# map("state", "maryland")
# points(i, pch = 21, cex = 2)
# points(d[dist.i.5,][1:500,], pch = "+")

# Tested loop below, now moved to wazefunctions.R as makelink()
# linktable <- vector()
# starttime <- Sys.time()
# 
# for(i in 1:nrow(edt)){ # i = 1
#   ei = edt[i,]
#   dist.i <- spDists(ei, d, longlat = T)*0.6213712 # spDists gives units in km, convert to miles
#   dist.i.5 <- which(dist.i <= 0.5)
#   
#   # Spatially matching
#   d.sp <- d[dist.i.5,]
#   
#   # Temporally matching
#   d.t <- d.sp[d.sp$time > ei$CrashDate_Local-60*60 & d.sp$time <= ei$CrashDate_Local+60*60,] 
#   
#   id.edt <- rep(as.character(ei$ID), nrow(d.t))
#   uuid.waze <- as.character(d.t$uuid)
#   
#   linktable <- rbind(linktable, data.frame(id.edt, uuid.waze))
#   
#   if(i %% 100 == 0) {
#       timediff <- round(Sys.time()-starttime, 2)
#       cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
#       }
# }