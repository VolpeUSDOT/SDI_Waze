# Review Bellevue Network Data

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

codeloc <- "~/GitHub/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
output.loc <- file.path(wazeshareddir, "Output/WA")
data.loc <- file.path(wazeshareddir, "Data/Bellevue")

## Working on SDC
# user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
# localdir <- paste0(user, "/workingdata") # full path for readOGR
# 
# wazedir <- file.path(localdir, "WA", "Waze") # has State_Year-mo.RData files. Grab from S3 if necessary
# 
# setwd(localdir) 

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))
# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# RoadNetwork data ----
# Michelle regenerated a shapefile of Bellevue Roadnetwork by excluding the highway/freeway/interstate: RoadNetwork_Jurisdiction.shp
# Jessie convert it to an txt file: RoadNetwork_Jurisdiction.csv
roadnettb <- read.csv(file = file.path(data.loc, "Shapefiles/RoadNetwork_Jurisdiction.csv"))
names(roadnettb)

# Rename the columns based on the full names
names(roadnettb) <- c("FID",
                      "OBJECTID",
                      "StreetSegmentID",
                      "LifeCycleStatus",
                      "OfficialStreetName",
                      "FromAddressLeft",
                      "FromAddressRight",
                      "ToAddressLeft",
                      "ToAddressRight",
                      "LeftJurisdiction",
                      "RightJurisdiction",
                      "LeftZip",
                      "RightZip",
                      "OneWay",
                      "SpeedLimit",
                      "ArterialClassification",
                      "FunctionClassDescription",
                      "ArterialSweepingFrequencyCode",
                      "EmergencyEvacuationRoute",
                      "EmergencyResponsePriorityRoute",
                      "SnowResponsePriorityCode",
                      "TruckRouteNumber",
                      "IsAddressable",
                      "IsPrivate",
                      "IsAccessRoad",
                      "AnomalyType",
                      "StreetNameAnno",
                      "StreetBlockNumber",
                      #"SHAPE", # do not exist in csv
                      "Shape_STLength"
)


# ArterialClaffication, FunctionClassDescription
count_AC <- roadnettb %>% group_by(ArterialClassification) %>% summarize(n = n()
                                                             , length_ft = mean(Shape_STLength))

count_FC <- roadnettb %>% group_by(FunctionClassDescription) %>% summarize(n = n()
                                                                       , length_ft = mean(Shape_STLength))

count_FC_AC <- roadnettb %>% group_by(ArterialClassification, FunctionClassDescription) %>% summarize(n = n(), length_ft = mean(Shape_STLength))

# Speedlimit
table(roadnettb$SpeedLimit)

# OneWay
table(roadnettb$OneWay) # what do "From" "To" imply?

# IsPrivate
table(roadnettb$IsPrivate)
