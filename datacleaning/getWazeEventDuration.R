

# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile

# Install libraries 
library(RPostgres)
library(getPass)
library(lubridate)
library(data.table)

# turn off scienctific notation 
options(scipen = 999)


# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))


# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))


# uncomment these lines and run with user redshift credentials filled in to resolve error if above line throws one.
#Sys.setenv('sdc_waze_username' = 'mgilmore') 
#Sys.setenv('sdc_waze_password' = 'r7ZcGYzjgmu4zxKR')


# Query parameters

# Time zone picker:
tzs <- data.frame("MD", tz = "US/Eastern", stringsAsFactors = F)



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
        '2017-09-10 09:00:00' and '2017-09-11 09:00:00'  --NOTE-UTC is 4 hours ahead of EDT, this gives us 5AM - 5AM 
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



alert_results <- dbGetQuery(conn, alert_query)

alert_save <- alert_results


alert_results$start_pub_millis <- as.numeric(alert_results$start_pub_millis)


alert_results$event_duration_minutes <- as.numeric(alert_results$sum_total_occurences * 2)


alert_results$est_end_pub_millis <- (alert_results$start_pub_millis + (alert_results$event_duration_minutes * 60000))


#alert_results <- subset(alert_results, select = c(1:19))


alert_results$end_pub_utc_timestamp <- as.POSIXct(as.numeric(alert_results$est_end_pub_millis)/1000, 
                                                  origin = "1970-01-01", tz = "GMT")



hr_breaks <- c(0, 1*3600, 2*3600, 3*3600, 4*3600, 5*3600, 6*3600, 
               7*3600, 8*3600, 9*3600, 10*3600, 11*3600, 12*3600,
               13*3600, 14*3600, 15*3600, 16*3600, 17*3600, 18*3600, 
               19*3600, 20*3600, 21*3600, 22*3600, 23*3600, 24*3600)

hr_breaks_lbs <- c("hr_00", "hr_01", "hr_02", "hr_03", "hr_04", "hr_05", "hr_06", 
                   "hr_07", "hr_08", "hr_09", "hr_10", "hr_11", "hr_12", 
                   "hr_13", "hr_14", "hr_15", "hr_16", "hr_17", "hr_18", 
                   "hr_19", "hr20", "hr_21", "hr_22", "hr_23")

sec.per.day <- 24*60*60

get.cuts <- function(x, y) as.list(table(cut((x:y)%%sec.per.day, breaks = hr_breaks, labels = hr_breaks_lbs))/60)

class(alert_results) = "data.table"

alert_results[,c(hr_breaks_lbs):=get.cuts(start_pub_utc_timestamp, end_pub_utc_timestamp), by=1:nrow(alert_results)]


alert_results$end_pub_utc_timestamp%%sec.per.day

class(alert_results)



setDT(alert_results)[,c("start_pub_utc_timestamp","end_pub_utc_timestamp"):=lapply(.SD, as.POSIXct, tz="GMT"), .SDcols=c(14, 19)]



#loop through each row 
#extract hour for start and end time 
#start time in minutes upto the next hour (difference)

#search for examples on lubridate per hour for multiple hours 











