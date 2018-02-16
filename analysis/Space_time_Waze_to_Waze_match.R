# Time-space match points
# Matching Waze events to other Waze events (needed to make indicator variables showing matches to toher Waze events).


# Original Goal: for a given EDT event, find the matching Waze events within 60 min on either side, within a 0.5 mi radius.
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.
# Now applying to all Waze events to Waze accidents, and updating to run on ATA server

# use spDists from sp package to get distances from each EDT event to each Waze event. 
# Produce a link table which has a two columns: EDT events and the Waze events which match them; repeat EDT event in the column for all matching Waze events.
# ** ATA note: needed to install.packages('sp', type = 'source') to resolve "error sp_dists not available for .C()". Then quit and re-start R after re-installing sp from source.

# Setup ----
library(sp)
library(tidyverse)
library(aws.s3)

LOCALTEST = T # F to run on ATA server, T to test on local machine

if(LOCALTEST){ 
  codeloc = "~/git/SDI_Waze/" 
  mappeddrive = "W:/"
  wazedir = file.path(mappeddrive, "SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped")
  load(file.path(wazedir,"MD_buffered__2017-04.RData"))
  } else { 
    
  codeloc <- "~/SDI_Waze"
  setwd("~/")
  # Ensure AWS credentials are set up. If this fails, set up AWS Access Keys in the AWS Console on a web browser, then save the plain text file of the key and secret in a secure location.
  # in the Jupyter terminal, use `aws configure` and enter those values by pasting into the console. 
  aws.signature::use_credentials()
  
  # load data from S3 bucket
  s3load("working/MD_buffered__2017-04.RData", bucket = "ata-waze")
  }

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# linking all Waze events to all other Waze events, for April. Set the projection to match that of the census data we use as the default
proj4string(d) <- c("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")

if(LOCALTEST){ d <- d[sample(1:nrow(d), size = 500),]} # small sample for local test

# <><><><><><><><><><><><><><><><><><><><><><><><>

link.waze.waze <- makelink(accfile = d,
                           incfile = d,
                           acctimevar = "time",
                           inctimevar1 = "time",
                           inctimevar2 = "last.pull.time",
                           accidvar = "uuid",
                           incidvar = "uuid"
                           )

# <><><><><><><><><><><><><><><><><><><><><><><><>

link.waze.wazeAll <-  link.waze.waze[as.character(link.waze.waze[,1]) != as.character(link.waze.waze[,2]),]

# remove self-matches
link.waze.wazeAll <-  link.waze.wazeAll[link.waze.wazeAll[,1] != link.waze.wazeAll[,2],]

write.csv(link.waze.wazeAll, "Waze_WazeAll_link_April_MD.csv", row.names = F)

s3save(link.waze.wazeAll, "Waze_WazeAll_link_April_MD.RData", bucket = "ata-waze")
s3save(link.waze.wazeAll, "Waze_WazeAll_link_April_MD.RData", bucket = "ata-sdi-out")
