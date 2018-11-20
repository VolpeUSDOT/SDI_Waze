setwd("~/workingdata/TN")

# Setup ----
# If you don't have these packages: install.packages(c("maps", "sp", "rgdal", "rgeos", "tidyverse")) 
# ggmap: want development version: devtools::install_github("dkahle/ggmap")
library(maps)
library(sp)
library(rgdal)
library(rgeos)
library(ggmap)
library(spatstat) # for ppp density estimation. devtools::install_github('spatstat/spatstat')
library(tidyverse)

codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

wazedir <- "~/workingdata/TN" # Waze data has State_Year-mo.RData files. Grab from S3 if necessary

setwd(localdir)

# load data ----

# stack Waze data , see Hot_spot_test.R.
# Event state
state = "TN"

wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]

use.tz <- "US/Central" # depend on the time zone of the state.

tn.waze <- vector()

for(i in 1:length(wazemonthfiles)){ # Start loop; i = 1
  
  load(file.path(wazedir, wazemonthfiles[i]))
  tn.waze <- rbind(tn.waze, mb)
}

# Convert Waze data to spatial data frame.

# Waze data dictionary: shared drive/documentation/data_dictionary.xlsx

# format special event/create comparison date (Non-event Days), look for Hot_Spot_Multiple_Events.R

