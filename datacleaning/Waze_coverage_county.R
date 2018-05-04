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


# Looping over States ----

stateresults <- vector()
starttime = Sys.time()

source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))

for(i in states){
  # read data by state for processing
  alert_query <- paste0("SELECT DISTINCT alert_uuid, location_lat, location_lon, street, city, state, pub_utc_timestamp, sub_type
                        FROM alert WHERE state='", i,
                        "' AND alert_type='ACCIDENT' 
                        AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
                        " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths.end)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
  
  results <- DBI::dbGetQuery(conn, alert_query) 
  
  stateresults <- rbind(stateresults, results)
  
  cat(format(nrow(results), big.mark = ","), "observations in", i, "\n")
  timediff <- Sys.time() - starttime
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed \n\n")
  save("results", file = paste0(i, "_accidents_distinct.RData"))
  
}

# Convert values to numeric
numconv <- c("location_lon", "location_lat")

stateresults[numconv] <- apply(stateresults[numconv], 2, function(x) as.numeric(x))

save("stateresults", file = "All_State_accidents_distinct.RData")


dbDisconnect(conn) #  disconnect to prevent leakage


#### Spatial aggregation ----
# read in county shapefiles
# Loop over full data frame if possible, or over states, and produce counts of Waze accidents by some time frame (perhaps daily, 365 * ~ 3000 = ~ 1 million rows output)

# grab file from S3 bucket if not already available locally (to do)

load("All_State_accidents_distinct.RData") # .5 Gb, 30 sec to load. Dataframe is stateresults


# Get counties
co <- rgdal::readOGR("/home/daniel/workingdata", "cb_2017_us_county_500k")

# go over stateresults and apply COUNTYFP to each value

xx <- SpatialPoints(coords = stateresults[c("location_lon", "location_lat")])

proj4string(xx) = proj4string(co)


system.time(x1 <- over(xx, co[c("STATEFP", "COUNTYFP")]))

d <- data.frame(stateresults, x1)

d$yearday <- format(d$pub_utc_timestamp, "%Y-%m-%d")
d$pub_utc_timestamp <- as.character(d$pub_utc_timestamp)


# with complete(), fills all combinations of statefp, countyfp, and yearday, including missing values (so we can fill with 0). 1,126,993 rows for 2017-03-07 to 2018-04-18.
# without complete() term, much faster, and produces ~290,000 row data frame.
d2 <- d %>%
  group_by(STATEFP, COUNTYFP, yearday) %>%
  summarise(WazeAccidents = n(),
            WazeAccidents_Major = sum(sub_type == "ACCIDENT_MAJOR"),
            WazeAccidents_Minor = sum(sub_type == "ACCIDENT_MINOR")) %>%
  complete(STATEFP, COUNTYFP, yearday, 
           fill = list(WazeAccidents = 0,
                       WazeAccidents_Major = 0,
                       WazeAccidents_Minor = 0))

county.acc <- left_join(d2, co@data[c("COUNTYFP", "STATEFP","NAME", "ALAND","AWATER")], 
                by = c("STATEFP", "COUNTYFP"))

d2.nofill <- d %>%
  group_by(STATEFP, COUNTYFP, yearday) %>%
  summarise(WazeAccidents = n(),
            WazeAccidents_Major = sum(sub_type == "ACCIDENT_MAJOR"),
            WazeAccidents_Minor = sum(sub_type == "ACCIDENT_MINOR"))

county.acc.nofill <- left_join(d2.nofill, co@data[c("COUNTYFP", "STATEFP","NAME", "ALAND","AWATER")], 
                        by = c("STATEFP", "COUNTYFP"))

save(list = c("county.acc", "county.acc.nofill"), file="~/tempout/Yearday_County_Waze_AccidentCounts.RData")

system("aws s3 cp /home/daniel/tempout/Yearday_County_Waze_AccidentCounts.RData s3://prod-sdc-sdi-911061262852-us-east-1-bucket/CountyCounts/")

# to do: spread across all possible year-day values in the sample, providing 0's



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
# 12 minutes only. much better not in a loop
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


