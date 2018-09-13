

# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile

# Install libraries 
library(dplyr)
library(RPostgres)
library(getPass)
library(lubridate)
library(data.table)
library(sp)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles

# turn off scienctific notation 
options(scipen = 999)


# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))


# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))


# uncomment these lines and run with user redshift credentials filled in to resolve error if above line throws one.
#Sys.setenv('sdc_waze_username' = 'mgilmore') 
#Sys.setenv('sdc_waze_password' = 'r7ZcGYzjgmu4zxKR')


# Set up workstation to get hexagon shapefiles 
source(file.path(codeloc, 'utility/Workstation_setup.R'))


### Waze Alert Query parameters

# Time zone picker:
tzs <- data.frame("MD", tz = "US/Eastern", stringsAsFactors = F)


### NOTE: BE SURE TO CHANGE THE DATE IN THE QUERY (line 64)

alert_query <- paste0(
  "SELECT     
    alert_uuid, sum_total_occurences, alert_type, sub_type, street, city, state, country, 
    location_lat, location_lon, start_pub_millis, start_pub_utc_timestamp, pub_millis, 
    pub_utc_timestamp, pub_utc_epoch_week, road_type
  FROM (
      SELECT 
        a.alert_uuid, b.sum_total_occurences, a.alert_type, a.sub_type, a.street, 
        a.city, a.state, a.country, a.location_lat, a.location_lon, a.start_pub_millis, 
        a.start_pub_utc_timestamp, a.pub_millis, a.pub_utc_timestamp, a.pub_utc_epoch_week, a.road_type, 
        ROW_NUMBER() OVER (PARTITION BY a.alert_uuid ORDER BY a.start_pub_millis) AS ROW_NUM
      FROM dw_waze.alert a
      JOIN ( 
        SELECT 
          alert_uuid, SUM(total_occurences) AS sum_total_occurences 
        FROM dw_waze.alert 
        WHERE state = 'MD'
        AND start_pub_utc_timestamp BETWEEN 
        --'2017-09-10 04:00:00' and '2017-09-11 04:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 12AM - 12AM 
        '2017-09-17 04:00:00' and '2017-09-18 04:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 12AM - 12AM 
        AND alert_type IN ('JAM', 'ACCIDENT')
        GROUP BY alert_uuid 
      ) b 
      ON a.alert_uuid = b.alert_uuid
      WHERE a.state = 'MD'
      GROUP BY 
        a.alert_uuid, b.sum_total_occurences, a.alert_type, a.sub_type, a.street, a.city, a.state,
        a.country, a.location_lat, a.location_lon, a.start_pub_millis, a.start_pub_utc_timestamp, 
        a.pub_millis, a.pub_utc_timestamp, a.pub_utc_epoch_week, a.road_type
  )
  WHERE ROW_NUM = 1
  ORDER BY start_pub_millis desc")


# run query to get waze events 
alert_results <- dbGetQuery(conn, alert_query)

alert_save <- alert_results # save incase of issues 


# convert start_pub_millis to numeric (start epoch/unix time in milliseconds since 1970-01-01)
alert_results$start_pub_millis <- as.numeric(alert_results$start_pub_millis)


# calculate estimated duration of the event in minutes 
# multiply the number of total occurences by 2 (2 = assume receive a Waze report every 2 minutes)
alert_results$event_duration_minutes <- as.numeric(alert_results$sum_total_occurences * 2)


# calculate the estimated end time of an event using the start time and duration (reported in epoch/unix time) 
alert_results$estimated_end_pub_millis <- (alert_results$start_pub_millis + (alert_results$event_duration_minutes * 60000))


# convert estimated_end_pub_millis into a datetime value 
alert_results$end_pub_utc_timestamp <- as.POSIXct(as.numeric(alert_results$estimated_end_pub_millis)/1000, 
                                                  origin = "1970-01-01", tz = "GMT")


# convert UTC to EST 
#alert_results$start_pub_edt_timestamp <- as.POSIXct(alert_results$start_pub_utc_timestamp, tz ="GMT")
#attributes(alert_results$start_pub_edt_timestamp)$tzone <- "America/New_York"


#alert_results$end_pub_edt_timestamp <- as.POSIXct(alert_results$end_pub_utc_timestamp, tz = "GMT")
#attributes(alert_results$end_pub_edt_timestamp)$tzone <- "America/New_York"


# Only want to bin the minutes which take place during the specific day, 
# but sometimes an event carries over into the next day and fall outside  
# the hourly bins and therefore those overextending minutes need to be 
# omitted from the binning calculation. 

### epoch time for end of day on Septemeber 10, 2017 - Event Day 
#end_of_day <- as.POSIXct(as.numeric(1505102399000)/1000, origin = "1970-01-01", tz = "GMT")


### epoch time for end of day on September 17, 2017 - No Event Day
end_of_day <- as.POSIXct(as.numeric(1505707199000)/1000, origin = "1970-01-01", tz = "GMT")


alert_results$cut_eod_utc_timestamp <- alert_results$end_pub_utc_timestamp


# replace date.time values past the end of the day with the end_of_day value. 
alert_results <- within(alert_results, 
                         cut_eod_utc_timestamp <- replace(cut_eod_utc_timestamp, 
                                                          cut_eod_utc_timestamp > end_of_day, 
                                                          end_of_day))

# set data.frame to also be a data.table 
# NOTE: THIS IS REQUIRED TO RUN get.cuts FUNCTION ON alert_results 
# example from: https://stackoverflow.com/questions/32469829/r-calculate-time-within-time-interval-between-start-end-time 
setDT(alert_results)[,c("start_pub_utc_timestamp","cut_eod_utc_timestamp"):=lapply(.SD, as.POSIXct, tz="GMT"), .SDcols=c(12,20)]


### intervals for which the event data will be cut (e.g. divided), multiplied by the number of seconds per hour 
### NOTE: there is one more break value than interval levelss because need the end interval value for hour 23 (e.g. 24*3600)

### 12AM - 12 AM
hr_breaks <- c(0, 1*3600, 2*3600, 3*3600, 4*3600, 5*3600, 6*3600, 
               7*3600, 8*3600, 9*3600, 10*3600, 11*3600, 12*3600, 
               13*3600, 14*3600, 15*3600, 16*3600, 17*3600, 18*3600, 
               19*3600, 20*3600, 21*3600, 22*3600, 23*3600, 24*3600)


  
### interval level labels 
hr_breaks_lbs <- c("hr_00", "hr_01", "hr_02", "hr_03", "hr_04", "hr_05", "hr_06", 
                   "hr_07", "hr_08", "hr_09", "hr_10", "hr_11", "hr_12", 
                   "hr_13", "hr_14", "hr_15", "hr_16", "hr_17", "hr_18", 
                   "hr_19", "hr_20", "hr_21", "hr_22", "hr_23")


sec.per.day <- 24*60*60

# function to bin the duration of an event by the number of minutes per hour over multiple hours 
get.cuts <- function(x, y) as.list(table(cut((x:y)%%sec.per.day, 
                                             breaks = hr_breaks, 
                                             labels = hr_breaks_lbs,
                                             ordered_result = TRUE, 
                                             include.lowest = TRUE))/60)


# run function using start and end timestamp fields for each event (row) -- NOTE CURRENTLY STILL IN UTC TIME 
alert_results_bin <- alert_results[,c(hr_breaks_lbs):=get.cuts(start_pub_utc_timestamp, cut_eod_utc_timestamp), by=1:nrow(alert_results)]



#### start working with Spatial data 

localdir <- "/home/mgilmore/workingdata/" # local directory where data is located

# MD hexagon shapefile and supporting files 
hex = readOGR(file.path(localdir, "Hex", "shapefiles"), layer = "MD_hexagons_1mi_newExtent_newGRIDID")


gridcols <- names(hex)[grep("^GRID", names(hex))] # hexagon column names


# convert lat/lon columns to numeric 
alert_results_bin$location_lon <- as.numeric(alert_results_bin$location_lon)
alert_results_bin$location_lat <- as.numeric(alert_results_bin$location_lat)


alert_results_bin <- as.data.frame(alert_results_bin) # convert table back to data.frame 


# Project to Albers equal area conic 102008. Check comparision with USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version, used for producing hexagons: 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"



# project Waze data into same coordinate reference system as hexagons (USGS version of Albers equal area; WKID: 102039)
alert_results_bin <- SpatialPointsDataFrame(alert_results_bin[c("location_lon", "location_lat")], alert_results_bin, 
                                        proj4string = CRS("+proj=longlat +datum=WGS84"))  

alert_results_bin <-spTransform(alert_results_bin, CRS(proj.USGS))


# Overlay Waze events into grid cells 
waze_hex <- over(alert_results_bin, hex[,gridcols]) # Match hexagon names to each row in alert_results_bin 

alert_results_bin@data <- data.frame(alert_results_bin@data, waze_hex)

waze_hex_df <- alert_results_bin@data # save as a data.frame 


# aggregate the number of minutes per hour, per event type, per grid cell 
# (e.g. calculate event duration, in minutes, per hour, per grid cell) 


#### Grouped by grid id and alert type (jams, accidents)
#output_df_0910 <- waze_hex_df %>% 
output_df_0917 <- waze_hex_df %>% 
  group_by(GRID_ID, alert_type) %>%
  summarise(
    hr_04 = sum(hr_04), 
    hr_05 = sum(hr_05), 
    hr_06 = sum(hr_06), 
    hr_07 = sum(hr_07), 
    hr_08 = sum(hr_08), 
    hr_09 = sum(hr_09), 
    hr_10 = sum(hr_10), 
    hr_11 = sum(hr_11), 
    hr_12 = sum(hr_12), 
    hr_13 = sum(hr_13), 
    hr_14 = sum(hr_14), 
    hr_15 = sum(hr_15), 
    hr_16 = sum(hr_16), 
    hr_17 = sum(hr_17), 
    hr_18 = sum(hr_18), 
    hr_19 = sum(hr_19), 
    hr_20 = sum(hr_20), 
    hr_21 = sum(hr_21),
    hr_22 = sum(hr_22), 
    hr_23 = sum(hr_23), 
    hr_00 = sum(hr_00), 
    hr_01 = sum(hr_01), 
    hr_02 = sum(hr_02), 
    hr_03 = sum(hr_03)
  )


# rename columns from UTC to EDT (GMT-4) values --- (BEFORE = hr_04 - hr_03)   (AFTER = hr_00 - hr_23) 
names(output_df_0917) <- c("GRID_ID", "alert_type",
                      "hr_00", "hr_01", "hr_02", "hr_03", "hr_04", "hr_05",
                      "hr_06", "hr_07", "hr_08", "hr_09", "hr_10", "hr_11", 
                      "hr_12", "hr_13", "hr_14", "hr_15", "hr_16", "hr_17", 
                      "hr_18", "hr_19", "hr_20", "hr_21", "hr_22", "hr_23")


### export data

fn = "Duration_EventDay_0910_12to12" # file name for day of event - September 10, 2017
write.csv(output_df_0910, file = file.path(localdir, paste0(fn, ".csv")), row.names = F) 

fn = "Duration_NoEventDay_0917_12to12" # file name for day with no event - September 17, 2017
write.csv(output_df_0917, file = file.path(localdir, paste0(fn, ".csv")), row.names = F)


# copy from instance to se3 bucket 
system(paste("aws s3 cp", 
             file.path(localdir, paste0(fn, ".csv")),
             file.path(teambucket, 'export_requests', paste0(fn, ".csv")))) 



# NOTE: To delete a file on s3, type the following into the command line: 
  # aws s3 rm s3://prod-sdc-sdi-911061262852-us-east-1-bucket/<folder>/<file.name>
