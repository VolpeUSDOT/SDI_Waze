# Waze coverage by county. Query Redshift database for one state at a time, overlay on county shapefile, summarize counts of Waze events by county, by day if possible 
library(tidyverse) 
library(lubridate)
library(rgdal)
library(sp)
library(raster)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "prod-sdc-sdi-911061262852-us-east-1-bucket"
output.loc <- "~/tempout"
codeloc <- "~/SDI_Waze"

# Connect to Redshift 
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))
# must dbDisconnect(conn) after every connection to prevent slowdowns

states <- c(state.abb, paste0("CA", 1:3))

### Query parameters ----
# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths = c(
  paste(2017, formatC(3:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:4, width = 2, flag = "0"), sep="-")
)

yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

# Test query by month vs entire timeframe
# 2.03 hr for RI. No accidents until October

state = "RI"

stateresults <- vector()
starttime = Sys.time()

for(i in 1:length(yearmonths)){ # i = 1
  
  # read data by state for processing
  alert_query <- paste0("SELECT location_lat, location_lon, street, city, pub_utc_timestamp, sub_type
                        FROM alert WHERE state='", state,
                        "' AND alert_type='ACCIDENT' 
                        AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[i], 
                        " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[i], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
  
  
  results <- DBI::dbGetQuery(conn, alert_query) # can be ~ 2-3 min per month; consider moving the summarise steps below to the SQL query.
  
  
  
  stateresults <- rbind(stateresults, results)
  
  cat(yearmonths[i], "queried, ", format(nrow(results), big.mark = ","), "observations \n")
  timediff <- Sys.time() - starttime
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed \n")
  
}  
    
# Convert values to numeric
numconv <- c("location_lon", "location_lat")

stateresults[numconv] <- apply(stateresults[numconv], 2, function(x) as.numeric(x))

save("stateresults", file = paste(state, "accidents.RData", sep="_"))

dbDisconnect(conn) # Must disconnect to prevent leakage


# State results, no loop ----
state = "RI"

stateresults <- vector()
starttime = Sys.time()

source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))

# read data by state for processing
alert_query <- paste0("SELECT DISTINCT alert_uuid, location_lat, location_lon, street, city, pub_utc_timestamp, sub_type
                      FROM alert WHERE state='", state,
                      "' AND alert_type='ACCIDENT' 
                      AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
                      " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths.end)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query

results <- DBI::dbGetQuery(conn, alert_query) 

stateresults <- rbind(stateresults, results)

cat(format(nrow(results), big.mark = ","), "observations \n")
timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed \n")

# Convert values to numeric
numconv <- c("location_lon", "location_lat")

stateresults[numconv] <- apply(stateresults[numconv], 2, function(x) as.numeric(x))

save("stateresults", file = paste(state, "accidents_distinct.RData", sep="_"))



dbDisconnect(conn) # Must disconnect to prevent leakage


# State count of alerts. See Waze_coverage.Rmd ----
state_alert_query <- paste0("SELECT COUNT(DISTINCT alert_uuid), state, alert_type
                            FROM alert 
                            WHERE pub_utc_timestamp BETWEEN to_timestamp('2017-04-01 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('2018-03-30 23:59:59','YYYY-MM-DD HH24:MI:SS')
                            GROUP BY state, alert_type;")
system.time(results <- dbGetQuery(conn, state_alert_query))

write.csv(results, "~/workingdata/State_Alert-Type_Count.csv", row.names= F)



# Test states in Redshift
# Need to extract location_lat, location_lon, street, city, pub_utc_timestamp, alert_subtype 
# completely by state
# Where alert_type = ACCIDENT
# First, get state names
state_name_query <- paste0("SELECT DISTINCT state
                           FROM alert 
                           WHERE pub_utc_timestamp BETWEEN to_timestamp('2018-03-30 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('2018-03-30 23:59:59','YYYY-MM-DD HH24:MI:SS');")

# 2+ hours... only 33 states represented that day. 
# After resetting system, still 43 min for this query
# system.time(results <- dbGetQuery(conn, state_name_query))
# dbDisconnect(conn)


