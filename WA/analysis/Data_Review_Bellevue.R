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
# 10. Weather data
###############################################################################################
# There are two versions of the shapefiles,:
# the first series was based on data collected till Nov 2018, these has been put in Shapefiles\Archive folder.
# the second was based on calendar year 2018 per Bellevue's request.
###############################################################################################

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
states = c("WA")
tzs <- data.frame(states, 
                  tz = c("US/Eastern"),
                  stringsAsFactors = F)

# Project to NAD_1983_2011_StatePlane_Washington_North_FIPS_4601_Ft_US, (Well-Known)WKID: 6597
# USGS ID: 102039 # this is for the hexogan.
proj <- showP4(showWKT("+init=epsg:6597"))
# proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0" 
# Hexogon grids need a flat space, and USGS is more commonly used for that purpose. We want to keep the Bellevue network in WKID 6597 as Bellevue is already using it.
network <- readOGR(dsn = file.path(data.loc, "Roadway","OSTSafetyInitiative_20181121"), layer = "RoadNetwork")
network <- spTransform(network, CRS(proj)) # 10320*28, it has 10,320 segments in the original data sent to us.
plot(network)

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
roadnettb <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction")
roadnettb <- roadnettb@data
dim(roadnettb) # 6647   29
names(roadnettb)

# Rename the columns based on the full names
names(roadnettb) <- c("OBJECTID",
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
                      "Shape_STLength",
                      'RDSEG_ID'
)

# ArterialClaffication, FunctionClassDescription
count_AC <- roadnettb %>% 
  filter(!is.na(ArterialClassification)) %>%
  group_by(ArterialClassification) %>% 
  summarize(n = n(),
            length_ft = mean(Shape_STLength, na.rm=T))

count_FC <- roadnettb %>% 
  filter(!is.na(FunctionClassDescription)) %>%
  group_by(FunctionClassDescription) %>%
  summarize(n = n(),
            length_ft = mean(Shape_STLength))

count_FC_AC <- roadnettb %>%
  filter(!is.na(FunctionClassDescription) & !is.na(ArterialClassification)) %>%
  group_by(ArterialClassification, FunctionClassDescription) %>% 
  summarize(n = n(),
            length_ft = mean(Shape_STLength))

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
# format of time
range(as.Date(crashtb$DATE, format = "%m/%d/%Y")) # "2017-04-01" "2018-11-16"
# crashtb$DATE <- as.Date(crashtb$DATE, format = "%m/%d/%Y")
crashtb$datetime <- as.POSIXct(paste(crashtb$DATE, crashtb$X24.HR.TIME), tz = 'America/Los_Angeles', format = "%m/%d/%Y %H:%M")
# there two timezone came out: one is PDT (Pacific Daylight Time) and PST (Pacific Standard Time)
# unique identifier
sum(duplicated(crashtb$REPORT.NUMBER)) # 0

# 3. Crash Points that has been filtered and snapped to a filtered segment ----
# load Shapefiles\Archive\CrashReports_Snapped50ft_MatchName.shp (WKID: 6597)
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "CrashReports_Snapped50ft_MatchName")
crash_snapped <- spTransform(crash_snapped, CRS(proj)) # 2085 rows * 29 columns
# Shapefiles\CrashReports_Snapped50ft_MatchName_withIntersections.shp
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "CrashReports_Snapped50ft_MatchName_withIntersections") # 2085 rows * 30 columns
crash_snapped <- spTransform(crash_snapped, CRS(proj))

plot(crash_snapped, col = "red")

# 3/27 update, new data Michelle prepared for calendar year 2018
crash_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Crashes_Snapped50ft_MatchName") # 2085*29, new: 1369*51 
crash_snapped <- spTransform(crash_snapped, CRS(proj))
names(crash_snapped) # some columns names are truncated, we can retrieve them from the raw data.
# [1] "JURISDICTI" "COUNTY"     "CITY"       "REPORT_NUM" "INDEXED_PR" "PRIMARY_TR" "BLOCK_NUMB" "MILEPOST"   "A_B"       
# [10] "INTERSECTI" "DIST_FROM_" "MI_or_FT"   "COMP_DIR_F" "REFERENCE_" "DATE"       "YEAR"       "QTR__"      "MONTH"     
# [19] "F24_HR_TIM" "FULL_TIME"  "MOST_SEVER" "MOST_SEV_1" "TOTAL_CRAS" "FATAL_CRAS" "SERIOUS_IN" "EVIDENT_IN" "POSSIBLE_I"
# [28] "PDO___NO_I" "TOTAL_FATA" "TOTAL_SERI" "TOTAL_EVID" "TOTAL_POSS" "TOTAL_VEHI" "TOTAL_PEDE" "TOTAL_BICY" "WORKZONE"  
# [37] "FIRST_COLL" "SECOND_COL" "JUNCTION_R" "WEATHER"    "ROAD_SURFA" "LIGHTING_C" "MISC_TRAFF" "COUNTY_RD_" "ARM"       
# [46] "RDSEG_ID"   "StreetSegm" "OfficialSt" "HourOfDay"  "MinOfDay"   "INT_ID" 

# Unique ID
sum(duplicated(crash_snapped@data$REPORT_NUM)) # 0
sum(duplicated(crash_snapped@data$RDSEG_ID)) # 786, many crashes to one segment relationship
sum(duplicated(crash_snapped@data$INT_ID)) # 1102

# 4. Waze event points ----
wazetb <- read.csv(file = file.path(data.loc, "Export","WA_Bellevue_Prep_Export.csv"))
names(wazetb) # "lat", "lon", "alert_type", "time", "roadclass", "sub_type", "city", "street", "magvar"
dim(wazetb) # 637629*9
names(wazetb)
# [1] "lat"        "lon"        "alert_type" "time"       "roadclass"  "SDC_uuid"   "sub_type"   "city"       "street"    
# [10] "magvar" 
# the time column is in "2017-04-04 16:30:29" format

# the new Waze data for calendar year 2018
wazetb <- read_csv(file.path(data.loc, 'Export', 'WA_Bellevue_Prep_Export.csv')) # same name.
wazetb$time <- as.POSIXct(wazetb$time, tz = "America/Los_Angeles")

# 5. Shapefiles of waze points that link the segments ----
# Shapefiles\Archive\WazeReports_Snapped50ft_MatchName.shp
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "WazeReports_Snapped50ft_MatchName")
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

# how many unique segments are Waze events link?
length(unique(waze_snapped@data$NEAR_FID)) # 1,846, 28% of all segments

# overlay crash and waze events
plot(waze_snapped, col = "blue")
plot(crash_snapped, col = "red", add=T)

# 3/27 update, new data Michelle prepared for calendar year 2018
waze_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Waze_Snapped50ft_MatchName") # 114614*15, new: 70226*17
waze_snapped <- spTransform(waze_snapped, CRS(proj))
names(waze_snapped@data)
# [1] "lat"        "lon"        "alert_type" "time"       "roadclass"  "SDC_uuid"   "sub_type"   "city"       "street"    
# [10] "magvar"     "NEAR_X"     "NEAR_Y"     "RDSEG_ID"   "StreetSegm" "OfficialSt" "HourOfDay"  "MinOfDay"  
# RDSEG_ID is now the unique ID of the streetlayer data.
# SDC_uuid is the unique id of each Waze event.
# "magvar"     "NEAR_X"     "NEAR_Y" should be intermediate columns created by Michelle.
# HourOfDay is in 24 hour format

table(waze_snapped@data$roadclass) # ~10% of Waze events are missing roadclass.
# 1    17     2    20     6     7    NA 
# 1282     6 10410    54 31425 20468  6581

# Date time format
range(as.Date(waze_snapped@data$time, format = "%Y/%m/%d")) # "2017-12-31" "2018-12-31"

# 6. Road network with additional calculated columns----
# Shapefiles\Archive\RoadNetwork_Jurisdiction_withIntersections_FullCrash.shp
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "RoadNetwork_Jurisdiction_withIntersections_FullCrash") # 6647 * 14
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

# If all segments have unique ID, the only unique IF is the objectedID, which does not link to the original data. Suggest that we gave all segments a unique ID and carrier it over to the subset.
length(unique(roadnettb_snapped$OBJECTID)) # 6647

plot(roadnettb_snapped)

# 3/27 update, new data Michelle prepared for calendar year 2018
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
roadnettb_snapped <- spTransform(roadnettb_snapped, CRS(proj))
names(roadnettb_snapped@data) # OBJECTID carried over from the raw data. RDSEG_ID is the unique ID we created.
# [1] "OBJECTID"   "StreetSegm" "LifeCycleS" "OfficialSt" "FromAddres" "FromAddr_1" "ToAddressL" "ToAddressR" "LeftJurisd"
# [10] "RightJuris" "LeftZip"    "RightZip"   "OneWay"     "SpeedLimit" "ArterialCl" "FunctionCl" "ArterialSw" "EmergencyE"
# [19] "EmergencyR" "SnowRespon" "TruckRoute" "IsAddressa" "IsPrivate"  "IsAccessRo" "AnomalyTyp" "StreetName" "StreetBloc"
# [28] "Shape_STLe" "RDSEG_ID"   "nWaze_All"  "nWazeAcc"   "End1_IntID" "End2_IntID" "nCrashes"   "Crash_End1" "Crash_End2"
# [37] "nBikes"     "nFARS_1217"
length(unique(roadnettb_snapped@data$RDSEG_ID)) #6647
is.unsorted(roadnettb_snapped@data$RDSEG_ID) # TRUE, the IDs are not sorted.
range(as.numeric(roadnettb_snapped@data$RDSEG_ID)) # from 1 to 6647, great!

# 7. Shapefiles of intersections, linked with segment ID (important) ----
#	Shapefiles\Archive\Intersections_withSegmentIDs.shp  
seg_int_link <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "Intersections_withSegmentIDs") #
seg_int_link <- spTransform(seg_int_link, CRS(proj))
names(seg_int_link@data)
# [1] "Int_ID"     "StreetSeg1" "StreetSeg2" "StreetSeg3" "StreetSeg4" "StreetSeg5" "StreetSeg6" # ordered by the segment ID from smallest to largest.
plot(seg_int_link)

# 8. Emergency response geo-coordinate data from NORCOM ----
# Shapefiles\Archive\Tableau_WazeReports_CrashReports_NORCOM_Merged.shp
norcom <- readOGR(dsn = file.path(data.loc, "Shapefiles", "Archive"), layer = "Tableau_WazeReports_CrashReports_NORCOM_Merged") # 8795 rows * 8 columns
norcom <- spTransform(norcom, CRS(proj))
names(norcom@data)
# [1] "OBJECTID"   "DATE"       "NEAR_FID"   "OfficialSt" "StreetSegm" "HourOfDay"  "MinOfDay"   "Dataset" 

plot(norcom)

# The Norcom data is not complete, we are waiting to get a better data (csv format), and do a snap.

# 9 Bike/Ped Conflict data ----
# Shapefiles\Wikimap_Snapped50ft.shp 
# only distance snap was applied to this data, will be needed in the aggregation code
Bike_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "Wikimap_Snapped50ft") # 1402*14
Bike_snapped <- spTransform(Bike_snapped, CRS(proj))
names(Bike_snapped)
# [1] "id"         "user_id"    "layer_id"   "created"    "sub_id"     "like_disli" "survey"     "kml_ID"     "kml_name"  
# [10] "kml_descri" "kml_icon"   "RDSEG_ID"   "OfficialSt" "StreetSegm"
sum(duplicated(Bike_snapped@data$id)) # id is an unique ID
sum(duplicated(Bike_snapped@data$user_id)) # 768 duplicates, might be joined from another data, what are users?
sum(duplicated(Bike_snapped@data$layer_id)) # 1397 duplicates, might be joined from another data, what layers?
sum(duplicated(Bike_snapped@data$kml_ID)) # unique, looks like this comes from the kml file, was classified to different types
table(Bike_snapped@data$kml_name) # we can use this information to create counts of each type of events for each segment as variables.
# Bicycle Accommodation Issues  Unsafe Behavior by Bicyclists     Unsafe Behavior by Drivers Unsafe Behavior by Pedestrians 
# 477                             14                            418                             50 
# Walking Accommodation Issues 
# 443 

sum(duplicated(Bike_snapped@data$RDSEG_ID)) # 719 replicates, meaning that this is a many to one relationship.
# "OfficialSt" "StreetSegm" should come from the network data.

# created date range
range(as.Date(Bike_snapped@data$created)) #"2015-08-26" "2015-11-01", the data was created in 2015, would it be appropriate for modeling 2018 crash data?

# 10. weather data -- Only need to assign to a date
load(file.path(data.loc, "Weather","Prepared_Bellevue_Wx_2018.RData"))
dim(wx.grd.day) #3,766,800*7
names(wx.grd.day)
# [1] "day"  "ID"   "PRCP" "TMIN" "TMAX" "SNOW" "mo"
range(wx.grd.day$day) #"2018-01-01" "2018-12-31"
range(wx.grd.day$mo) # month
summary(wx.grd.day)
