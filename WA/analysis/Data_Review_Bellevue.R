##### Review Bellevue Data ####
# 1. RoadNetwork (street layer) data, highway excluded
# 2. Raw Crash points - potential to add additional variables to the final model
# 3. Crash Points shapefile that has been filtered and matched with a filtered segment
# 4. Waze event points
# 5. Shapefiles of waze points that link the segments
# 6. Road network with additional calculated columns
# 7. Shapefiles of intersections, linked with segment ID (important)
# 8. Emergency response geo-coordinate data from NORCOM
# 9. Bike/Ped Location data

# Setup ----
# If you don't have these packages: install.packages(c("maps", "sp", "rgdal", "rgeos", "tidyverse")) 
# ggmap: want development version: devtools::install_github("dkahle/ggmap")
rm(list= ls())
library(maps) # for mapping base layers
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(rgeos) # gintersection
library(ggmap)
library(spatstat) # for ppp density estimation. devtools::install_github('spatstat/spatstat')
library(tidyverse)

codeloc <- "~/GitHub/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
output.loc <- file.path(wazeshareddir, "Output/WA")
data.loc <- file.path(wazeshareddir, "Data/Bellevue")

## Working on SDC - comment as we will be mainly based off the shared drive instead of SDC
# user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
# localdir <- paste0(user, "/workingdata") # full path for readOGR
# 
# wazedir <- file.path(localdir, "WA", "Waze") # has State_Year-mo.RData files. Grab from S3 if necessary
# 
# setwd(localdir) 

# Spatial data process and basic layers ----
# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Time zone picker:
states = c("MD")
tzs <- data.frame(states, 
                  tz = c("US/Eastern"),
                  stringsAsFactors = F)

# Project to NAD_1983_2011_StatePlane_Washington_North_FIPS_4601_Ft_US, (Well-Known)WKID: 6597
# USGS ID: 102039
proj <- showP4(showWKT("+init=epsg:6597"))
# proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0" 
# Hexogon grids need a flat space, and USGS is more commonly used for that purpose. We want to keep the Bellevue network in WKID 6597 as Bellevue is already using it.

city <- readOGR(dsn = file.path(data.loc, "Roadway","OSTSafetyInitiative_20181121"), layer = "CityBoundary")
city <- spTransform(city, CRS(proj))
plot(city)

PedFac <- readOGR(dsn = file.path(data.loc, "Roadway","OSTSafetyInitiative_20181121"), layer = "PedestrainFacilities")
PedFac <- spTransform(PedFac, CRS(proj)) # 4232*10
plot(PedFac, add = T, col = "grey")

zoning <- readOGR(dsn = file.path(data.loc, "Roadway","OSTSafetyInitiative_20181121"), layer = "Zoning")
zoning <- spTransform(zoning, CRS(proj)) # 983*22
plot(zoning, add = T, col = "grey")


# 1. RoadNetwork (street layer) data, highway excluded ----
# Michelle regenerated a shapefile of Bellevue Roadnetwork by excluding the highway/freeway/interstate: RoadNetwork_Jurisdiction.shp
# Jessie convert it to an txt file: RoadNetwork_Jurisdiction.csv
roadnettb <- read.csv(file = file.path(data.loc, "Shapefiles/RoadNetwork_Jurisdiction.csv"))
dim(roadnettb) # 6647   29
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

# 2. Raw Crash points - potential to add additional variables to the final model----
crashtb <- read.csv(file = file.path(data.loc, "Crash","20181127_All_roads_Bellevue.csv"))
names(crashtb) # Looks like this is a full crash database
dim(crashtb) # 4417  254, a total of 4417 crashes.

# Columns that may be of interest, the first 45 columns
crashtb <- crashtb[,1:45]
str(crashtb)
nacounts <- colSums(is.na(crashtb)) # Milepost, Dista.From.ref.point, COUNTY.RD.ONLY..INTERSECTING.CO.RD.MILEPOST, ARM have some NAs, all other columns are clean.

# 3. Crash Points that has been filtered and matched with a filtered segment ----
# load Shapefiles\CrashReports_Snapped50ft_MatchName.shp (WKID: 6597)
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "CrashReports_Snapped50ft_MatchName")
crash_snapped <- spTransform(crash_snapped, CRS(proj)) # 2085 rows * 29 columns
# Shapefiles\CrashReports_Snapped50ft_MatchName_withIntersections.shp
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "CrashReports_Snapped50ft_MatchName_withIntersections") # 2085 rows * 30 columns
crash_snapped <- spTransform(crash_snapped, CRS(proj))

plot(crash_snapped, col = "red")

# 4. Waze event points ----
wazetb <- read.csv(file = file.path(data.loc, "Export","WA_Bellevue_Prep_Export.csv"))
names(wazetb) # "lat", "lon", "alert_type", "time", "roadclass", "sub_type", "city", "street", "magvar"
dim(wazetb) # 637629*9
# the time column is in "2017-04-04 16:30:29" format

# 5. Shapefiles of waze points that link the segments ----
# Shapefiles\WazeReports_Snapped50ft_MatchName.shp
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "WazeReports_Snapped50ft_MatchName")
waze_snapped <- spTransform(waze_snapped, CRS(proj))
#  114614 rows * 15 columns (18% of Waze were snapped to the segments)
names(waze_snapped@data) # where does the added columns come from? segment layer?
# [1] "OBJECTID"   "lat"        "lon"        "alert_type" "time"       "roadclass"  "sub_type"   "city"      
# [9] "street"     "magvar"     "NEAR_FID"   "OfficialSt" "StreetSegm" "HourOfDay"  "MinOfDay"
# NEAR_FID is the ObjectID of the streetlayer data.

# the time column is in "2017/04/11" format, we need the full timestamp, at least the month of the Waze event.
table(waze_snapped@data$alert_type)
# ACCIDENT           JAM   ROAD_CLOSED WEATHERHAZARD 
# 1908         84826          2637         25243

# how many unique segment are Waze events link?
length(unique(waze_snapped@data$NEAR_FID)) # 1,846, 28% of all segments

# overlay crash and waze events
plot(waze_snapped, col = "blue")
plot(crash_snapped, col = "red", add=T)

# 6. Road network with additional calculated columns
# Shapefiles\RoadNetwork_Jurisdiction_withIntersections_FullCrash.shp
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withIntersections_FullCrash") # 6647 * 14
roadnettb_snapped <- spTransform(roadnettb_snapped, CRS(proj))
names(roadnettb_snapped@data)
# [1] "OBJECTID"   "StreetSegm" "OfficialSt" "OneWay"     "SpeedLimit" "ArterialCl" "FunctionCl" "Length_FT" 
# [9] "End1_IntID" "End2_IntID" "nWz_Acc"    "nCrashes"   "nCrashEnd1" "nCrashEnd2"  

# a.	nWz_Acc  =  the total number of Waze accidents on that segment.  
# b.	nCrashes  =  the total number of Bellevue reported crashes on that segment.  
# c.	End1_IntID  =  the intersection ID for the first (e.g. starting point) of that segment.  
# d.	End2_IntID  =  the intersection ID for the second (e.g. ending point) of that segment.  
# e.	nCrashEnd1  =  the number of Bellevue reported crashes on that segment associated with the first intersection (End1_IntID). 
# f.	nCrashEnd2  =  the number of Bellevue reported crashes on that segment associated with the section intersection (End2_IntID).  

# If a IntID = 0, it is NULL.

plot(roadnettb_snapped)
     
# 7. Shapefiles of intersections, linked with segment ID (important) ----
#	Shapefiles\Intersections_withSegmentIDs.shp  
seg_int_link <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Intersections_withSegmentIDs") #
seg_int_link <- spTransform(seg_int_link, CRS(proj))
names(seg_int_link@data)
# [1] "Int_ID"     "StreetSeg1" "StreetSeg2" "StreetSeg3" "StreetSeg4" "StreetSeg5" "StreetSeg6" # ordered by the segment ID from smallest to largest.
plot(seg_int_link)

# 8. Emergency response geo-coordinate data from NORCOM ----
# Shapefiles\Tableau_WazeReports_CrashReports_NORCOM_Merged.shp
norcom <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Tableau_WazeReports_CrashReports_NORCOM_Merged") # 8795 rows * 8 columns
norcom <- spTransform(norcom, CRS(proj))
names(norcom@data)
# [1] "OBJECTID"   "DATE"       "NEAR_FID"   "OfficialSt" "StreetSegm" "HourOfDay"  "MinOfDay"   "Dataset" 

plot(norcom)

# The Norcom data is not complete, we are waiting to get a better data (csv format), and do a snap.

# 9 Bike/Ped Conflict data (Raw) ----
# Michelle may need to snap it using some columns, such as location description.

