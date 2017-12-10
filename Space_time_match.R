# Time-space match points
# Test of matching EDT and Waze events, first using 5 mi square data from Maryland, April 2017.

# Setup ----
library(sp)
library(tidyverse)
library(maps)

# Code location
codeloc <- "~/Documents/git/SDI_Waze"

# load data - will load all files in month_MD_clipped directory on shared drive
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

# use spDists from sp package to get distances from each EDT event to each Waze event. Could pre-filter Waze events by lat long; a degree latitude or longitude is ~ 69 mi for Maryland. If it is too slow, try this.

# Produce a link table which has a two columns: EDT events and the Waze events which match them; repeat EDT event in the column for all matching Waze events.

# link.test <- makelink(edt[1:250,], d[1:5000,])

link.all <- makelink(edt, d)

write.csv(link.all, "EDT_Waze_link_April_MD.csv", row.names = F)

# Next up: making summary tables from the link table

# EDT files with matching Waze events:
length(unique(fivemi.link$id.edt))
# out of
nrow(edt)
# and with this distribution of Waze matches
hist(tapply(fivemi.link$uuid.waze, fivemi.link$id.edt, length))

# Erika code:

###Some code for subsetting EDT and Waze data based on link table
#Subset EDT data to April
edt.april <- edt[edt$CrashDate_Local < "2017-05-01 00:00:00 EDT" & edt$CrashDate_Local > "2017-04-01 00:00:00 EDT",] 
edtfile <- edt.april
edt.april$CrashDate_Local

#Use link function to match April EDT and April Waze
link.5mi <- makelink(edt.april, d)
write.csv(link.5mi, "EDT_Waze_link_April_MD_5mi.csv", row.names = F)

#Add "M" code to the table to show matches if we merge in full datasets
match <- rep("M",nrow(link.5mi))
link.5mi <- mutate(link.5mi, match)
glimpse(link.5mi)

#Subset of Waze data that matches EDT
Waze.Match <- subset(d, (uuid %in% link.5mi$uuid.waze))
length(unique(Waze.Match$uuid)) #431

##This merge doesn't work: non-unique matches detected. We want to keep non-unique matches
WazeLinked <- merge(Waze.Match, link.5mi, by.x = "uuid", by.y = "uuid.waze")

#Subset of Waze data that does not match (check to be sure is same as total length April Waze - number of unique match Waze)
length(unique(link.5mi$uuid.waze))
nrow(d)
Waze.NoMatch <- subset(d, !(uuid %in% link.5mi$uuid.waze))
nrow(Waze.NoMatch) #431/9921 or only 4.3% of Waze events match an EDT crash in space and time

#check total
nrow(Waze.NoMatch)+length(unique(link.5mi$uuid.waze)) #Matches total number 

# EDT files with matching Waze events:
length(unique(link.5mi$id.edt))
# out of
nrow(edt.april) #70/96 in April match a Waze event (72.9%)

# and with this distribution of Waze matches
hist(tapply(link.5mi$uuid.waze, link.5mi$id.edt, length))





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