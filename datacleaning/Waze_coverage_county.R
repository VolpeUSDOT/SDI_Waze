# Waze coverage by county. Query Redshift database for one state at a time, combine to one large data frame. Overlay on county shapefile, summarize counts of Waze events by county, by day. 
library(tidyverse) 
library(lubridate)
library(rgdal)
library(sp)
library(raster)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "prod-sdc-sdi-911061262852-us-east-1-bucket"
output.loc <- "~/tempout"
codeloc <- "~/covid_waze"

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata/") # full path for readOGR

# Connect to Redshift 
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))
# must dbDisconnect(conn) after every connection to prevent slowdowns

# CA has four regions, CA, CA1, CA2, CA3. 
states <- c(state.abb, paste0("CA", 1:3))

### Query parameters ----
# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
# Originally set up for monthly queries; can consider now not limiting the time in the query
yearmonths = c(
  paste(2017, formatC(3:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:12, width = 2, flag = "0"), sep="-"),
  paste(2019, formatC(1:12, width = 2, flag = "0"), sep="-"),
  paste(2020, formatC(1:3, width = 2, flag = "0"), sep="-")
)

yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")


# Looping over States ----

stateresults <- vector()
starttime = Sys.time()

for(i in states){
  state_file = file.path(output.loc, paste0(i, "_events_distinct.RData"))
  # Check to see query ran before, if so just append to stateresults
  if(file.exists(state_file)){
    
   # load(state_file)
  } else {
    # read data by state for processing. Use current_flag=1 for deduplication. Total_occurrences for time; don't extract now. Also don't extract uuid, save some space
    alert_query <- paste0("SELECT state, alert_type, sub_type, location_lat, location_lon, pub_utc_timestamp, road_type FROM dw_waze.alert WHERE state='", i,
                          "' AND current_flag=1 AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
                          " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths.end)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
    
    # alert_query <- paste0("SELECT * FROM dw_waze.alert WHERE state='", i,
    #                       "' AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
    #                       " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths.end)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
    
    results <- DBI::dbGetQuery(conn, alert_query) 
 
    cat(format(nrow(results), big.mark = ","), "observations in", i, "\n")
    timediff <- Sys.time() - starttime
    cat(round(timediff, 2), attr(timediff, "units"), "elapsed \n\n")
    save("results", file = state_file)
    rm(results); gc()
  }
  
  # stateresults <- rbind(stateresults, results)
  
}
 
# # Convert values to numeric
# numconv <- c("location_lon", "location_lat")
# 
# stateresults[numconv] <- apply(stateresults[numconv], 2, function(x) as.numeric(x))
# 
# # save("stateresults", file = file.path(output.loc, paste0("All_State_Daily_Alerts_to_", Sys.Date(),".RData")))

dbDisconnect(conn) #  disconnect to prevent leakage
gc()

#### Spatial aggregation ----
# read in county shapefiles
# Loop over data frame for each state separately, and produce counts of Waze alerts 
# grab file from S3 bucket if not already available locally (to do)

# load("All_State_accidents_distinct.RData") # .5 Gb, 30 sec to load. Dataframe is stateresults


# Get counties
co <- rgdal::readOGR("/home/daniel/workingdata/census", "cb_2017_us_county_500k")

# go over stateresults and apply COUNTYFP to each value
# use maps::state.fips to confirm only counties in this state being used.
compiled_counts <- vector()

starttime = Sys.time()

for(i in states){
  state_file = file.path(output.loc, paste0(i, "_events_distinct.RData"))
  # Check to see query ran before, if so just append to stateresults
  load(state_file)
  
  # Find the FIPS code for this state. We will use this to filter out non-state data
  use_fips = usmap::fips(i)
  
  numconv <- c("location_lon", "location_lat")

  results[numconv] <- apply(results[numconv], 2, function(x) as.numeric(x))

  xx <- SpatialPoints(coords = results[c("location_lon", "location_lat")])
  
  proj4string(xx) = proj4string(co)
  
  x1 <- over(xx, co[c("STATEFP", "COUNTYFP")])

  d <- data.frame(results, x1)

  d <- d %>% filter(STATEFP == use_fips)
  
  d$yearday <- format(d$pub_utc_timestamp, "%Y-%m-%d")
  d$hour <- format(d$pub_utc_timestamp, "%H")
  
  d$pub_utc_timestamp <- as.character(d$pub_utc_timestamp)

  county_count <- d %>%
    filter(!is.na(COUNTYFP)) %>%
    group_by(STATEFP, COUNTYFP, yearday, hour, alert_type) %>%
    summarise(count = n()) 

  compiled_counts <- rbind(compiled_counts, as.data.frame(county_count))
  
  timediff = Sys.time() - starttime
  cat(i, 'completed in', round(timediff, 2), attr(timediff, 'units'), '\n')
}

# Save ----
# Write to .RData and .csv, place in team bucket.
# Also, zip together and place in export_request

save(list = c("compiled_counts"), file = file.path(output.loc, "Compiled_county_counts.RData"))

system(paste0("aws s3 cp ", file.path(output.loc,  "Compiled_county_counts.RData"), " s3://prod-sdc-sdi-911061262852-us-east-1-bucket/CountyCounts/"))

write.csv(compiled_counts, file = file.path(output.loc, "Compiled_county_counts.csv"), row.names = F)

system(paste0("aws s3 cp ", file.path(output.loc,  "Compiled_county_counts.csv"), " s3://prod-sdc-sdi-911061262852-us-east-1-bucket/CountyCounts/"))

filestozip = file.path(output.loc, 
                       c("Compiled_county_counts.RData",
                         "Compiled_county_counts.csv"))

zipname = paste0('Compiled_county_counts_', Sys.Date(), '.zip')

system(paste('zip -j', file.path(output.loc, zipname),
             paste(filestozip, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path(output.loc, zipname),
  file.path(teambucket, 'export_requests', zipname)
))

system("aws s3 ls prod-sdc-sdi-911061262852-us-east-1-bucket/")
