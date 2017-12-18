# Time-space match points
# Matching EDT and Waze events.

# Goal: for a given EDT event, find the matching Waze events within 60 min on either side, within a 0.5 mi radius.
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.

# use spDists from sp package to get distances from each EDT event to each Waze event. 
# Produce a link table which has a two columns: EDT events and the Waze events which match them; repeat EDT event in the column for all matching Waze events.

# Setup ----
library(sp)
library(tidyverse)
library(maps)

# Code location
mappeddriveloc <- "W:"

codeloc <- "~/git/SDI_Waze"
wazedir <- file.path(mappeddriveloc, "SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped")
edtdir <- file.path(mappeddriveloc, "SDI Pilot Projects/Waze/MASTER Data Files/EDT_month")


# load data - will load all files in month_MD_clipped directory on shared drive
# source(file.path(codeloc, 'wazeloader.R'))

setwd(wazedir)

# read functions
source(file.path(codeloc, 'wazefunctions.R'))

# Start loop over months ----
wazemonthfiles <- dir(wazedir)[grep("MD_buffered__2017", dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
                               
edtmonths <- unique(substr(dir(edtdir), 6, 7))
wazemonths <- unique(substr(wazemonthfiles, 19, 20))

months_shared <- edtmonths[edtmonths %in% wazemonths]
# make sure months are actually shared 
stopifnot(all(months_shared == wazemonths[wazemonths %in% edtmonths]))

outputdir_temp <- tempdir()
outputdir_final <- file.path(wazedir, "MASTER Data Files/EDT_month")

starttime_total <- Sys.time()

for(i in months_shared){
  starttime_month <- Sys.time()
  
  load(file.path(wazedir, paste0("MD_buffered__2017-", i, ".RData")))
  load(file.path(edtdir, paste0("2017-", i, "_1_CrashFact_edited.RData")))

  # Set projections
  # d <- SpatialPointsDataFrame(d[c("lon", "lat")], d)  # from monthbind of Waze data, make sure it is a SPDF
  edt <- get(paste("edt", i, sep = "_"))
  
  proj4string(d) <- proj4string(edt) <- c("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")
  
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  link.all <- makelink(edt, d)
  # <><><><><><><><><><><><><><><><><><><><><><><><>
  
  
  write.csv(link.all, file.path(outputdir_temp, paste0("EDT_Waze_link_2017-", i, "_MD.csv")), row.names = F)
  
  timediff <- round(Sys.time()-starttime_month, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")

  timediff.total <- round(Sys.time()-starttime_total, 2)
  cat(timediff.total, attr(timediff.total, "units"), "elapsed in total \n", rep("<>", 20), "\n")
  
}

filelist <- dir(outputdir_temp)[grep("[RData$|csv$]", dir(outputdir_temp))]
movefiles(filelist, outputdir_temp, outputdir_final)



# Next up: making summary tables from the link table

# EDT files with matching Waze events:
length(unique(fivemi.link$id.edt))
# out of
nrow(edt)
# and with this distribution of Waze matches
hist(tapply(fivemi.link$uuid.waze, fivemi.link$id.edt, length))



# linking Waze accidents to all Waze events, for April
proj4string(d) <- c("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")

wazeacc <- d[d$type == "ACCIDENT",]




# <><><><><><><><><><><><><><><><><><><><><><><><>
link.waze.waze <- makelink(wazeacc, d,
                           acctimevar = "time",
                           inctimevar1 = "time",
                           inctimevar2 = "last.pull.time",
                           accidvar = "uuid",
                           incidvar = "uuid")
# <><><><><><><><><><><><><><><><><><><><><><><><>

link.waze.waze2 <-  link.waze.waze[as.character(link.waze.waze[,1]) != as.character(link.waze.waze[,2]),]

write.csv(link.waze.waze2, "Waze_Waze_link_April_MD.csv", row.names = F)




# <><><><><><><><><><><><><><><><><><><><><><><><>
link.edt.edt <- makelink(edt, edt,
                           acctimevar = "CrashDate_Local",
                           inctimevar1 = "CrashDate_Local",
                           inctimevar2 = "CrashDate_Local",
                           accidvar = "ID",
                           incidvar = "ID")
# <><><><><><><><><><><><><><><><><><><><><><><><>

# remove self-matches

link.edt.edt2 <-  link.edt.edt[link.edt.edt[,1] != link.edt.edt[,2],]

write.csv(link.edt.edt2, "EDT_EDT_link_April_MD.csv", row.names = F)


# old ----

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


# Old Erika code -- all moved to UrbanArea_overlay.R, just keeping here for reference
# 
# ###Some code for subsetting EDT and Waze data based on link table
# #Subset EDT data to April
# edt.april <- edt[edt$CrashDate_Local < "2017-05-01 00:00:00 EDT" & edt$CrashDate_Local > "2017-04-01 00:00:00 EDT",] 
# edtfile <- edt.april
# edt.april$CrashDate_Local
# 
# #Use link function to match April EDT and April Waze
# link.5mi <- makelink(edt.april, d)
# write.csv(link.5mi, "EDT_Waze_link_April_MD_5mi.csv", row.names = F)
# 
# #Add "M" code to the table to show matches if we merge in full datasets
# match <- rep("M",nrow(link.5mi))
# link.5mi <- mutate(link.5mi, match)
# glimpse(link.5mi)
# 
# #Subset of Waze data that matches EDT
# Waze.Match <- subset(d, (uuid %in% link.5mi$uuid.waze))
# length(unique(Waze.Match$uuid)) #431
# 
# ##This merge doesn't work: non-unique matches detected. We want to keep non-unique matches
# WazeLinked <- merge(Waze.Match, link.5mi, by.x = "uuid", by.y = "uuid.waze")
# 
# 
# 
# 
# #Subset of Waze data that does not match (check to be sure is same as total length April Waze - number of unique match Waze)
# length(unique(link.5mi$uuid.waze))
# nrow(d)
# Waze.NoMatch <- subset(d, !(uuid %in% link.5mi$uuid.waze))
# nrow(Waze.NoMatch) #431/9921 or only 4.3% of Waze events match an EDT crash in space and time
# 
# #check total
# nrow(Waze.NoMatch)+length(unique(link.5mi$uuid.waze)) #Matches total number 
# 
# # EDT files with matching Waze events:
# length(unique(link.5mi$id.edt))
# # out of
# nrow(edt.april) #70/96 in April match a Waze event (72.9%)
# 
# # and with this distribution of Waze matches
# hist(tapply(link.5mi$uuid.waze, link.5mi$id.edt, length))
# 
