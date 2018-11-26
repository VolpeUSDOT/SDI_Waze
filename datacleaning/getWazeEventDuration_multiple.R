

<<<<<<< HEAD
#### PURPOSE - SELECT FOUR (4) STATES AND THE ACCIDENTS REPORTS FOR THE WHOLE YEAR ############
###############################################################################################

=======
>>>>>>> 4ae4862623157a16a2330fd1999a3979609f3961
# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile

<<<<<<< HEAD
=======
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

>>>>>>> 4ae4862623157a16a2330fd1999a3979609f3961
# Install libraries 
library(dplyr)
library(RPostgres)
library(getPass)
library(lubridate)
library(data.table)
library(sp)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
<<<<<<< HEAD
library(ggplot2)
=======
>>>>>>> 4ae4862623157a16a2330fd1999a3979609f3961

# turn off scienctific notation 
options(scipen = 999)


# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))


# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))


<<<<<<< HEAD
# uncomment these lines and run with user redshift credentials filled in to resolve error if above line throws one.
#Sys.setenv('sdc_waze_username' = 'mgilmore') 
#Sys.setenv('sdc_waze_password' = 'r7ZcGYzjgmu4zxKR')


=======
>>>>>>> 4ae4862623157a16a2330fd1999a3979609f3961
# Set up workstation to get hexagon shapefiles 
source(file.path(codeloc, 'utility/Workstation_setup.R'))


### Waze Alert Query parameters

<<<<<<< HEAD
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












=======
# Query parameters
# states with available EDT data for model testing
states = c("CT", "MD", "UT", "VA")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Eastern",
                         "US/Mountain",
                         "US/Eastern"),
                  stringsAsFactors = F)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling by State ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:8, width = 2, flag = "0"), sep="-")
)
yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

starttime <- Sys.time()

# For each state, grab already processed months of data with duration in there

all.state.acc = vector()

for(state in states){ # state = 'UT'
  
  # Check S3 contents for this state
  # system(paste("aws s3 ls",
  #              paste0(file.path(teambucket, state), "/")))
                
  # Get these months data from S3 and compile
  getmonths <- paste0(state, "_", yearmonths, ".RData")
  
  alert_results <- vector()
  for(i in getmonths){
    if(length(grep(i, dir(file.path('~', 'workingdata', state))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, state, i),
                   file.path('~', 'workingdata', state, i)))
    }
    load(file.path("~", "workingdata", state, i))
    alert_results <- rbind(alert_results, mb)
    
  }
  
  # calculate estimated duration of the event in minutes 
  # take difference of time and last.pull.time; correct pull durations for 2017 and 2018 have already been done

  timediff <- alert_results$last.pull.time - alert_results$time
  stopifnot(attr(timediff, "units")=="secs")
    
  alert_results$event_duration_minutes <- as.numeric(timediff/60)
  
  # Select columns to export for Tableau
  cols = c("lon", "lat", "city", "alert_type", "sub_type", "street", "road_type", "time", "last.pull.time", "event_duration_minutes")
  
  alert_results = alert_results %>% 
    filter(alert_type != "ROAD_CLOSED") %>%
    select(cols)
  
  # Looking at historgrams, limit to events lasting less than 3 hrs
  ggplot(alert_results %>% filter(event_duration_minutes < 60*3)) + 
    geom_histogram(aes(x=event_duration_minutes)) + 
    facet_wrap(~alert_type) + ggtitle(paste(state, "frequency of duration of events < 3 hrs"))
  ggsave(filename = paste0(file.path("~", "workingdata", "Figures/"), state, "_Event_Duration_Hist.jpg"))
  
  # Save to local, will zip and put in export after
  write.csv(alert_results, file = paste0(file.path(localdir, state, "/"), state, "_Event_Duration_to_", yearmonths[length(yearmonths)], ".csv", row.names = F))
  
  acc_res = alert_results %>%
    filter(alert_type == "ACCIDENT") %>%
    select(-alert_type) %>%
    mutate(state = state)
  
  all.state.acc = rbind(all.state.acc, acc_res)
  
}

# add day of week
all.state.acc$DayOfWeek = format(all.state.acc$time, "%A")

write.csv(all.state.acc, paste0(localdir, "/", "Accident_Duration_to_", yearmonths[length(yearmonths)], ".csv"),
          row.names=F)

# Zip and export

zipfiles = vector()
for(state in states){
  zipfiles = c(zipfiles, paste0(file.path(localdir, state, "/"), state, "_Event_Duration_to_", yearmonths[length(yearmonths)], ".csv"))
}

zipfiles = c(zipfiles, paste0(localdir, "/", "Accident_Duration_to_", yearmonths[length(yearmonths)], ".csv"))

zipname = paste0("Event_Accident_Durations_CT_MD_VA_UT_", Sys.Date(), ".zip")

for(z in zipfiles){
  system(paste('zip -j', file.path('~/workingdata', zipname), z))
}
  
system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

# Summarize for documentation
# all.state.acc <- read.csv("Accident_Duration_to_2018-08.csv", stringsAsFactors = F)

all.state.acc %>%
  group_by(state) %>%
#  count() %>%
  summarize(meanDur = mean(event_duration_minutes))
>>>>>>> 4ae4862623157a16a2330fd1999a3979609f3961
