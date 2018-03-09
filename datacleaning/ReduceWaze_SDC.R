# Aggregates Waze files to monthly sets with unique UUIDs. 
# Original 'ReduceWaze2.R' for working with daily .csv files, now directly reading from Redshift in SDC 

# setup ----

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
# library(aws.s3)
library(lubridate)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "prod-sdc-sdi-911061262852-us-east-1-bucket"
 
# aws.s3::get_bucket(output.loc) # forbidden 403, as aws.s3 relies on credentials, which we are not using. see: aws.signature::locate_credentials()

output.loc <- "~/tempout"

# read functionss
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Make connection to Redshift:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))

# Query parameters
year = 2017 # can be vector of multiple years
months = 3:12
state = "MD"

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling Monthly ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths <- paste(year, rep(formatC(months, width = 2, flag = "0"), length(year)), sep = "-")
yearmonths.1 <- paste(year, rep(formatC(months, width = 2, flag = "0"), length(year)), "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

starttime <- Sys.time()

# To do: convert to doParallel + foreach loop to spread this across CPUs.


for(i in 1:length(yearmonths)){ # i = 1
  
  # read data by month for processing
  alert_query <- paste0("SELECT * FROM alert WHERE state='", state,
                  "' AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[i], 
                  " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[i], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
  
  
  results <- dbGetQuery(conn, alert_query) # can be ~ 2-3 min per month; consider moving the summarise steps below to the SQL query.

  cat(yearmonths[i], "queried, ", format(nrow(results), big.mark = ","), "observations \n\n")
  
  # Convert values to numeric
  numconv <- c("report_rating", "confidence", "reliability", "pub_millis", "location_lon", "location_lat", "magvar")

  results[numconv] <- apply(results[numconv], 2, function(x) as.numeric(x))
    

  d <- as.tibble(results)
  
  d.test = d[1:5000,]
  
  ll <- d %>%
    group_by(alert_uuid) %>%
    summarise(
      median.reportRating = median(report_rating),
      median.confidence = median(confidence),
      median.reliability = median(reliability),
      pubMillis = min(pub_millis),
      lon = median(location_lon),
      lat = median(location_lat),
      last.time = max(pub_millis),
      nrecord = n()
    )
  
  # Additionally get confidence, reliability, report rating, and magvar for the most recent entry of this uuid. Can have fewer rows than part 1, ll.
  ll2 <-  d %>% 
    group_by(alert_uuid) %>%
    filter(pub_millis == max(pub_millis, na.rm = T)) %>%
    summarize(
      last.reportRating = median(report_rating),
      last.confidence = median(confidence),
      last.reliability = median(reliability),
      magvar = median(magvar, na.rm = T)
      
    )
  
  # part 3: get most frequent values for city, type, road_type, sub_type, and street. Fastest dplyr method is with slice(whic.max(n))
  # !!! Road type not in the Redshift database !! 
  
  ll3 <- d %>%
    group_by(alert_uuid) %>%
      count(city, alert_type, sub_type, street) %>%
         slice(which.max(n)) 
     
  # join these two three tables
  lx <- full_join(ll, ll2, by = 'alert_uuid')
  lx <- full_join(lx, ll3, by = 'alert_uuid')

  # Format and output  
  time <- as.POSIXct(lx$pubMillis/1000, origin = "1970-01-01", tz="America/New_York") # Time zone will need to be correctly configured for other States.
  last.pull.time <- as.POSIXct(lx$last.time/1000, origin = "1970-01-01", tz="America/New_York") 
  
  mb <- data.frame(lx, time, last.pull.time)
  
  filenamesave = paste("MD_",
                   yearmonths[i],
                   sep = "")
  
  write.csv(mb,
            file = file.path(output.loc, paste0(filenamesave, ".csv")),
            row.names = F)
  
  save(list = "mb", 
       file = file.path(output.loc, paste0(filenamesave, ".RData")))

  
  cat(format(nrow(mb), big.mark = ","), "observations in aggregated data \n")
  
  timediff <- round(Sys.time()-starttime, 2)
  cat(#yearmonths[i], "complete \n", 
    timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
  
  } # end month loop

# after completed, move these aggregated files from instance to S3 bucket

system("~/SDI_Waze/utility/sdc_s3_out.sh")

