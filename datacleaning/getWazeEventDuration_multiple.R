

#### PURPOSE - SELECT FOUR (4) STATES AND THE ACCIDENTS REPORTS FOR THE WHOLE YEAR ############
###############################################################################################

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
library(ggplot2)

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
  WHERE state in ('CT', 'MD', 'UT', 'VA')
  AND start_pub_utc_timestamp BETWEEN 
  --'2017-09-10 04:00:00' and '2017-09-11 04:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 12AM - 12AM 
  --'2017-09-17 04:00:00' and '2017-09-18 04:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 12AM - 12AM 
  '2017-04-01 04:00:00' and '2018-07-31 04:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 12AM - 12AM 
  AND alert_type IN ('ACCIDENT')
  GROUP BY alert_uuid 
  ) b 
  ON a.alert_uuid = b.alert_uuid
  WHERE a.state in ('CT', 'MD', 'UT', 'VA')
  GROUP BY 
  a.alert_uuid, b.sum_total_occurences, a.alert_type, a.sub_type, a.street, a.city, a.state,
  a.country, a.location_lat, a.location_lon, a.start_pub_millis, a.start_pub_utc_timestamp, 
  a.pub_millis, a.pub_utc_timestamp, a.pub_utc_epoch_week, a.road_type
  )
  WHERE ROW_NUM = 1
  ORDER BY start_pub_millis desc")


# run query to get waze events 
accident_duration_results <- dbGetQuery(conn, alert_query)

alert_save <- accident_duration_results # save incase of issues 


# convert start_pub_millis to numeric (start epoch/unix time in milliseconds since 1970-01-01)
accident_duration_results$start_pub_millis <- as.numeric(accident_duration_results$start_pub_millis)


# calculate estimated duration of the event in minutes 
# multiply the number of total occurences by 2 (2 = assume receive a Waze report every 2 minutes)
accident_duration_results$event_duration_minutes <- as.numeric(accident_duration_results$sum_total_occurences * 2)



# calculate the day of week of the event 
accident_duration_results$DayOfWeek <- weekdays(as.Date(accident_duration_results$start_pub_utc_timestamp))


# generate a unique id that is seperate from uuid 
accident_duration_results$generated_uid <- 1:nrow(accident_duration_results)


# subset to only fields which we are interested in: 
  # gerneated uid, state, time spane (event duration in minutes), event type and subtype, road type, day of week 
accident_duration_results <- accident_duration_results[c(19, 3, 4, 7, 18, 16, 17)]



### export data

fn = "AccidentDuration_April2017toJuly2018_CtMdUtVa" # file name 
write.csv(accident_duration_results, file = file.path(localdir, paste0(fn, ".csv")), row.names = F) 

system(paste(
  'zip ~/workingdata/AccidentDuration_CtMdUtVa.zip',
  '~/workingdata/AccidentDuration_April2017toJuly2018_CtMdUtVa.csv'))


system(paste(
  'aws s3 cp',
  '~/workingdata/AccidentDuration_CtMdUtVa.zip',
  file.path(teambucket, 'export_requests', 'AccidentDuration_CtMdUtVa.zip')
))




# NOTE: To delete a file on s3, type the following into the command line: 
# aws s3 rm s3://prod-sdc-sdi-911061262852-us-east-1-bucket/<folder>/<file.name>





ggplot(accident_duration_results) + geom_histogram(aes(x = event_duration_minutes), bins = 3)












