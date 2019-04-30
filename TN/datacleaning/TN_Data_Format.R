# Tennesee data initial formatting and exploration
# Read in raw data files, clean up variables, apply time zones, and save as .RData files for next steps.

# Setup ----
# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile
source(file.path(codeloc, 'utility/Workstation_setup.R')) # Download necessary files from S3

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(lubridate)
library(geonames) # for time zone work
library(httr)
library(readxl) # read excel filed
library(foreach)
library(doParallel)

# Customized Functions
sumstats = function(x) { 
  null.k <- function(x) sum(is.na(x))
  unique.k <- function(x) {if (sum(is.na(x)) > 0) length(unique(x)) - 1
    else length(unique(x))}
  range.k <- function(x) max(x, na.rm=TRUE) - min(x, na.rm=TRUE)
  mean.k=function(x) {if (is.numeric(x)) round(mean(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  sd.k <- function(x) {if (is.numeric(x)) round(sd(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  min.k <- function(x) {if (is.numeric(x)) round(min(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  q05 <- function(x) quantile(x, probs=.05, na.rm=TRUE)
  q10 <- function(x) quantile(x, probs=.1, na.rm=TRUE)
  q25 <- function(x) quantile(x, probs=.25, na.rm=TRUE)
  q50 <- function(x) quantile(x, probs=.5, na.rm=TRUE)
  q75 <- function(x) quantile(x, probs=.75, na.rm=TRUE)
  q90 <- function(x) quantile(x, probs=.9, na.rm=TRUE)
  q95 <- function(x) quantile(x, probs=.95, na.rm=TRUE)
  max.k <- function(x) {if (is.numeric(x)) round(max(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  
  sumtable <- cbind(as.matrix(colSums(!is.na(x))), sapply(x, null.k), sapply(x, unique.k), sapply(x, range.k), sapply(x, mean.k), sapply(x, sd.k),
                    sapply(x, min.k), sapply(x, q05), sapply(x, q10), sapply(x, q25), sapply(x, q50),
                    sapply(x, q75), sapply(x, q90), sapply(x, q95), sapply(x, max.k)) 
  
  sumtable <- as.data.frame(sumtable); names(sumtable) <- c('count', 'null', 'unique',
                                                            'range', 'mean', 'std', 'min', '5%', '10%', '25%', '50%', '75%', '90%',
                                                            '95%', 'max') 
  return(sumtable)
} 

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- if(length(grep("@securedatacommons.com", getwd())) > 0) {
  paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
} else {
  paste0( "/home/", system("whoami", intern = TRUE))
} # find the user directory to use

localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

setwd(localdir)
# setwd("//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Data/TN")

# Get Waze data ----

yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:12, width = 2, flag = "0"), sep="-"))

tn.ls = paste0("TN_", yearmonths, ".RData")

for(i in tn.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'TN', 'Waze'))))==0){
    
    system(paste("aws s3 cp",
                 file.path(teambucket, 'TN', i),
                 file.path('~', 'workingdata', 'TN', 'Waze', i)))
    
  }
}

# <><><><><><><><><><><><><><><><><><>
# Crash ----

# look at Crash/vwCollision.txt
# First time need to read from txt, afterwards can read from RData, ~ 2 seconds vs 5 minutes. Every column is read as character.
if(length(grep("TN_Crash.RData", dir('Crash')))==0){
  crash <- read.csv("TITAN_Crash_181108/vwCollision.txt", sep = '|', stringsAsFactors = F)
  # format(object.size(crash), 'Mb')
  save(crash, file = "Crash/TN_Crash.RData")
} else {
  load('Crash/TN_Crash.RData')
}

# examine the crash data
names(crash)
dim(crash) # 829,301 rows * 80 columns. 181108 version only 773,411 rows.

# Crash ID, it is unique? Yes.
length(unique(crash$MstrRecNbrTxt)) # this is a unique crash ID

table(crash$NbrUnitsNmb) # this column could include more than the number of vehicles
# 0      1      2      3      4      5      6      7      8      9     10     11     12     13     14     15     17     18     22    # 3 219063 565583  37956   5359    969    243     73     18      9      8      4      1      2      1      1      1      1      1    
# 26     98 
# 1      1

table(crash$CrashTypeCde) # need information from TN State Patrol for more information
# 0      1      2      3      4      5      6     80     98 
# 21   3627  21418  54944 114219 574395  60574     96      1 

# select the necessary columns for analysis (optional: save it as subset in the RData again?)
var <- c("MstrRecNbrTxt" # Unique crash ID
         , "CollisionDte" # Date of Crash
         , "CollisionTimeTxt" # Time of Crash
         , "NbrUnitsNmb" # Number of vehicles involved
         , "NbrFatalitiesNmb" # Fatals
         , "NbrInjuredNmb" # Injuries
         , "NbrNonInjuredNmb" # Non-Inguries
         , "NbrMotoristsNmb" # Number of drivers
         , "AlcoholInd" # whether alchohol is involved
         , "BlockNbrTxt" # Does not look like Census block
         , "CityCde" # City ID
         , "CountyStateCde" # County ID
         , "CrashTypeCde"  # What crash type do they have?
         , "IntersectionInd" # Whether it is an intersection             
         , "IntersectLocalIDTxt" # Intersection Location ID
         , "IntersectRoadNameTxt" # Intersection road name
         , "IntersectRoadNameSuffixTxt" # Name suffix
         , "IntersectRoadNbrTxt" # Intersection road/route number
         , "LatDecimalNmb" # Lat
         , "LongDecimalNmb" # Lon
         , "LightConditionCde" # light condition
         ) 
crash <- crash[,var]

# Data completeness
# crash[!complete.cases(crash),] # Error: C stack usage  8200812 is too close to the limit, the data is stil too large
# format(object.size(crash), "Mb") = 425 Mb

# Further reduce the data
var1 <- c("MstrRecNbrTxt", "CollisionDte", "LatDecimalNmb", "LongDecimalNmb") # only take ID, time, and location
crash1 <- crash[, var1]
crash1[!complete.cases(crash1),] # display rows with missing data, looks like most of them are just missing the geo-coordinates.

colSums(is.na(crash1)) # Number of missing data.
colSums(is.na(crash1))*100/nrow(crash1) # percent of missing data, ~29% missing Lat/Lon

# round(colSums(is.na(crash))*100/nrow(crash), 2) # Large amounts of data are missing for some columns, such as BlockNbrTxt, IntersectionInd, IntersectLocalIDTxt, IntersectRoadNameTxt. If we plan to use these columns, need to understand why the missing data are not captured in is.na(). This might be due to blanks. 


# Get columns in the right format
numcol <- grep("Nmb$", names(crash))
for(i in numcol){
  crash[,i] <- as.numeric(crash[,i])
  }

# Timezone ----
# Here applying a timezone shapefile. First, filter out bad lat/long values, and bad record numbers

crash <- crash %>% filter(LatDecimalNmb > 25 & LatDecimalNmb < 45 &
                               LongDecimalNmb < -75 & LongDecimalNmb > -99 &
                               nchar(MstrRecNbrTxt) == 9)

# Read tz file
tz <- readOGR(file.path(localdir, 'dist'), layer = 'combined-shapefile')

# Project to Albers equal area, ESRI 102008
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version of AEA. Use this for all projections for consistency; this is what the hexagon layer is in 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

tz <- spTransform(tz, CRS(proj.USGS))

# Project crashes

crash.proj <- SpatialPointsDataFrame(coords = crash[c('LongDecimalNmb', 'LatDecimalNmb')],
                                     data = crash,
                                     proj4string = CRS("+proj=longlat +datum=WGS84"))

crash.proj <- spTransform(crash.proj, CRS(proj.USGS))

# Overlay timezone on crashes
crash_tz <- over(crash.proj, tz[,"tzid"]) # Match a tzid name to each row in crash.proj 


crash.proj@data <- data.frame(crash.proj@data, tz = crash_tz)

crash <- crash.proj@data %>% filter(tzid == "America/Chicago" | tzid == "America/New_York")

pdf(file = 'Figures/Crash_TZ.pdf', width = 10, height = 10)
  par(mar = rep(0, 4))
  plot(crash.proj, cex = 0) 
  plot(crash.proj[crash.proj$tzid == "America/Chicago" ,], add = T, 
       col = scales::alpha("midnightblue", 0.01))
  plot(crash.proj[crash.proj$tzid == "America/New_York" ,], add = T, 
       col = scales::alpha("firebrick", 0.01))
  plot(crash.proj[crash.proj$tzid == "Antarctica/McMurdo" ,], add = T, col = "red", 
       cex = 3)
  plot(tz, add = T)
dev.off()

# Now make date/time using correct time zone. Need to loop because strptime does not accept vectors of mixed time zones. Parallelized.
crash$tzid = as.character(crash$tzid)

cl <- makeCluster(parallel::detectCores())
registerDoParallel(cl)

StartTime <- Sys.time()

foreach(i = 1:nrow(crash)) %dopar% {
  crash$date[i] = as.POSIXct(strptime(crash$CollisionDte[i], "%Y-%m-%d %H:%M:%S", 
                                      tz = crash$tzid[i]))
  crash$year[i] = as.numeric(format(crash$date[i], "%Y"))
  }
EndTime <- Sys.time() - StartTime
cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
stopCluster(cl)

crash %>% 
  group_by(tzid) %>%
  summarize(min = min(date),
            max = max(date),
            n = length(date))

save("crash", file = "Crash/TN_Crash_Simple_2008-2018.RData")

# Transfer the file to team bucket
system(paste("aws s3 cp",
             file.path(localdir, 'Crash',"TN_Crash_Simple_2008-2018.RData"),
             file.path(teambucket, "TN", "Crash", "TN_Crash_Simple_2008-2018.RData")))

# Additional summaries

na.lat <- crash %>%
  filter(!is.na(year)) %>%
  group_by(fatal = NbrFatalitiesNmb > 0 , year) %>%
  summarize(naLat = sum(is.na(LatDecimalNmb) | LatDecimalNmb <= -99),
            N = n(),
            pct.na = 100*naLat/N)

ggplot(na.lat) +
  geom_point(aes(x = as.factor(year), y = pct.na, color = fatal)) +
  ylab("Percent missing Lat/Long") + xlab("Year") + 
  ggtitle("Summary of missing location by year and crash fatality for Tennessee")
ggsave("Output/Missing_lat_TN.jpeg")

var1 <- c("MstrRecNbrTxt", "date", "year", "LatDecimalNmb", "LongDecimalNmb") # only take ID, time, and location
crash1 <- crash[, var1]

# sumstats(crash1[2:ncol(crash1)])

# Day of week and hour of day summaries
# 1 = Sunday here
crash$DayOfWeek <- lubridate::wday(crash$date, label = T)
crash$hour <- format(crash$date, "%H")

crash %>%
  filter(date > "2017-04-01") %>%
  group_by(DayOfWeek) %>%
  count()
  

byhr <- crash %>%
  filter(date > "2017-04-01") %>%
  group_by(hour) %>%
  count()
data.frame(byhr)

# <><><><><><><><><><><><><><><><><><>
# 2019 Special Events ----
# Includes all events from 2010-2019
spev <- read_excel("SpecialEvents/2019_Special_Events.xlsx")

spev <- spev %>% 
  filter(!is.na(Event_Date) & Event_Date >= '2019-01-01' & Event_Date <= '2019-12-31' &
         !is.na(Lat) & !is.na(Lon)) %>%
  mutate(Lat = as.numeric(Lat),
         Lon = as.numeric(Lon))
# The special event are from Jan 2019 - Dec 2019.

# spev$StartTime <- as.POSIXct(strptime(spev$StartTime, format = "%Y-%m-%d %H:%M:%S"))
spev$StartTime <- format(ymd_hms(spev$StartTime), "%H:%M:%S")
spev$EndTime <- format(ymd_hms(spev$EndTime), "%H:%M:%S")

# Assign time zone to each special event.
# Get timezone of event - TN has 2 timezones (eastern and central) - use geonames package 
spev$TimeZone <- NA

# Project spev
spev.proj <- SpatialPointsDataFrame(coords = spev[c('Lon', 'Lat')],
                                     data = spev,
                                     proj4string = CRS("+proj=longlat +datum=WGS84"))

spev.proj <- spTransform(spev.proj, CRS(proj.USGS))

# Overlay timezone on crashes
spev_tz <- over(spev.proj, tz[,"tzid"]) # Match a tzid name to each row in spev.proj 

spev.proj@data <- data.frame(spev.proj@data, tz = spev_tz)

spev <- spev.proj@data %>% filter(tzid == "America/Chicago" | tzid == "America/New_York")


## save special event data in the output
save("spev", file = "SpecialEvents/TN_SpecialEvent_2019.RData")

# <><><><><><><><><><><><><><><><><><>
# 2018 Special Events ----
spev <- read_excel("SpecialEvents/2018 Special Events.xlsx")

spev17 <- read_excel("SpecialEvents/EVENTS Updated 03_21_2017.xls")


dim(spev) #813*12
dim(spev17) # 670*8
# How many Event_Type
# 12 types:  [1] "Car Show"         "Fair"             "Fair/Festival"    "Festival"         "Motorcycle Rally" 
# "Parade"           "Race/Car Show"   "Rodeo"            "Run/Rally"        "Special"          "Special Event"    "Sporting"

table(spev$Event_Type)
table(spev17$Event_Type)

# Car Show             Fair    Fair/Festival         Festival Motorcycle Rally           Parade    Race/Car Show            Rodeo 
# 8              363               10              175                2               10                3                3 
# Run/Rally          Special    Special Event         Sporting 
# 13                5               81              139 

# what are the x__1 to x__4?
sum(is.na(spev$X__1)) # 811 missing, only two rows have data.
sum(is.na(spev$X__4)) # 812 missing

names(spev)
# [1] "Event"      "Event_Date" "Lat"        "Lon"        "StartTime"  "EndTime"    "Home_flg"   "Event_Type" "X__1"       "X__2"       "X__3"      
# [12] "X__4" 

# The special event are from Jan 2018 - Dec 2018. Date Range are numeric (in excel format), need to convert to dates.
# spev$Event_Date_format <- as.Date(as.numeric(spev$Event_Date), origin = "1899-12-30")
# sum(is.na(spev$Event_Date_format)) # one NA created
# spev$Event_Date[is.na(spev$Event_Date_format)]  # which row has NA?
spev$Event_Date[spev$Event_Date == "9/15/201/"] <- as.numeric(as.Date("2018-9-15") - as.Date(0, origin = "1899-12-30", tz = "UTC")) # excel day zero (1899-12-30) is -25569 in R day zero.

spev$Event_Date <- as.Date(as.numeric(spev$Event_Date), origin = "1899-12-30")
range(spev$Event_Date) # "2018-01-01" "2019-07-01", it actually has some special events in 2019.
sum(spev$Event_Date > "2018-12-31") # 3 special events in 2019

# How many rows missing start and end time
sum(is.na(spev$StartTime)) #643
sum(is.na(spev$EndTime)) #726

# Convert start and end time to regular time
ymd_hms("2019-12-31 12:04:23")

# spev$StartTime <- as.POSIXct(strptime(spev$StartTime, format = "%Y-%m-%d %H:%M:%S"))
spev$StartTime <- format(ymd_hms(spev$StartTime), "%H:%M:%S")
spev$EndTime <- format(ymd_hms(spev$EndTime), "%H:%M:%S")

# There are 363 special events in the data, wondering if this is a daily event at the same location?
table(spev$Event[spev$Event_Type == "Fair"]) # Scattered in different counties and at different date.

# Assign time zone to each special event.
# Get timezone of event - TN has 2 timezones (eastern and central) - use geonames package 
# NOTE: only need to run the next two lines once at the initial setup.  
options(geonamesUsername = "waze_sdi") # set this up as a generic username, only need to run this command once 
source(system.file("tests", "testing.R", package = "geonames"), echo = TRUE)

spev$TimeZone <- NA

## There are three rows do not have Lon/Lat, so need to skip them.
range <- which(!is.na(spev$Lat) & !is.na(spev$Lon))
for (i in range) {

  # spev$TimeZone[i] <- as.character(GNtimezone(spev$Lat[i], spev$Lon[i], radius = 0)$timezoneId)

}

## save special event data in the output
save("spev", file = "SpecialEvents/TN_SpecialEvent_2018.RData")

# <><><><><><><><><><><><><><><><><><>
# 2017 Special Event ----
spev = spev17

# How many rows missing start and end time
sum(is.na(spev$StartTime)) # 487
sum(is.na(spev$EndTime)) # 487

# spev$StartTime <- as.POSIXct(strptime(spev$StartTime, format = "%Y-%m-%d %H:%M:%S"))
spev$StartTime <- format(ymd_hms(spev$StartTime), "%H:%M:%S")
spev$EndTime <- format(ymd_hms(spev$EndTime), "%H:%M:%S")

# There are 363 special events in the data, wondering if this is a daily event at the same location?
table(spev$Event[spev$Event_Type == "Fair"]) # Scattered in different counties and at different date.

# Assign time zone to each special event.
# Get timezone of event - TN has 2 timezones (eastern and central) - use geonames package 
# NOTE: only need to run the next two lines once at the initial setup.  
options(geonamesUsername = "waze_sdi") # set this up as a generic username, only need to run this command once 
source(system.file("tests", "testing.R", package = "geonames"), echo = TRUE) # InternetOpenUrl failed: 'The server name or address could not be resolved'

spev$TimeZone <- NA

# Trim trailing white space from lat and long 
spev$Lat <- gsub("[[:space:]]", "", spev$Lat)
spev$Lon <- gsub("[[:space:]]", "", spev$Lon)

rowindex <- vector()
starttime = Sys.time()

for (i in 1:nrow(spev)) {

  if(spev$Lat[i] != 'all' & !is.na(spev$Lat[i])){
    query = paste0("http://api.geonames.org/timezoneJSON?lat=", round(as.numeric(spev$Lat[i]), 6), 4,
                   "&lng=", round(as.numeric(spev$Lon[i]), 6),
                   "&radius=0&username=waze_sdi")
  
    tzres <- httr::GET(query)
    if(http_error(tzres) | is.null( content(tzres)$timezoneId) ) {
      spev$TimeZone[i] <- "error"
    } else {
      spev$TimeZone[i] <- content(tzres)$timezoneId
    }
  } else {
    spev$TimeZone[i] = NA
  }

  # TimeZone <- c(TimeZone, tzres)
  # rowindex <- c(rowindex, i)

  if(i %% 100 == 0) cat(i, ". ")
  rowindex = c(rowindex, i) # just to be sure, keep track of row index i
}

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed")
# Five min for 2017, Took Jessie's laptop 1.99 (1st time) and 7.79 (2nd time) mins, this probably depending on what you have running.
# 2018-12-26: update from ws.geonames.org to api.geonames.org.

# Check NAs: these are 'all', shoudl apply to every grid ID
spev %>%
  filter(is.na(TimeZone))

# One particular location returned error, try to manually fix
spev %>%
  filter(TimeZone == 'error')

# Loop over row names where timezone is error. Convert from dplyr tibble format back to normal data frame to get rownames conveniently
# Problem was white space at end of one of the longitudes, now fixed.


## save special event data in the output
save("spev", file = "SpecialEvents/TN_SpecialEvent_2017.RData")

# step 3: Transfer the file to workingdata folder.
system(paste("aws s3 cp",
             file.path('~', 'workingdata', 'TN', 'SpecialEvents',"TN_SpecialEvent_2017.RData"),
             file.path(teambucket, "TN", "SpecialEvents", "TN_SpecialEvent_2017.RData")))

# Select a special event for the footprint analysis


# look at 'SpecialEvents/2018 Special Events.xlsx'; can use read_excel function in readxl package
# Waze data
load("TN_2017-04.RData") # load an example Waze data to look at columns
