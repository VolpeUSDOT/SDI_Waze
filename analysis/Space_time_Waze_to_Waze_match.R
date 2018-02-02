# Time-space match points
# Matching Waze events to other Waze events (needed to make indicator variables showing matches to toher Waze events).

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
mappeddriveloc <- "W:" #Flynn
mappeddriveloc <- "S:" #Sudderth


codeloc <- "~/git/SDI_Waze" #Flynn
codeloc <- "~/GitHub/SDI_Waze" #Sudderth

wazedir <- file.path(mappeddriveloc, "SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped")
edtdir <- file.path(mappeddriveloc, "SDI Pilot Projects/Waze/MASTER Data Files/EDT_month")


# load data - will load all files in month_MD_clipped directory on shared drive
# source(file.path(codeloc, 'wazeloader.R'))

setwd(wazedir)

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

#TODO: add code to load April Waze data (don't want to load all)

# linking all Waze events to all other Waze events, for April
proj4string(d) <- c("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")

wazeacc <- d[d$type == "ACCIDENT",]




# <><><><><><><><><><><><><><><><><><><><><><><><>
link.waze.waze <- makelink(d, d,
                           acctimevar = "time",
                           inctimevar1 = "time",
                           inctimevar2 = "last.pull.time",
                           accidvar = "uuid",
                           incidvar = "uuid")
# <><><><><><><><><><><><><><><><><><><><><><><><>

link.waze.wazeAll <-  link.waze.waze[as.character(link.waze.waze[,1]) != as.character(link.waze.waze[,2]),]

# remove self-matches
link.waze.wazeAll <-  link.waze.wazeAll[link.waze.wazeAll[,1] != link.waze.wazeAll[,2],]

write.csv(link.waze.wazeAll, "Waze_WazeAll_link_April_MD.csv", row.names = F)

