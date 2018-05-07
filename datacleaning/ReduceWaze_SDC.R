# Aggregates Waze files to monthly sets with unique UUIDs. 
# Original 'ReduceWaze2.R' for working with daily .csv files, now directly reading from Redshift in SDC. Modified to query multiple states
# This now calls Waze_clip.R to subset to buffered state polygons before making monthly files by unique alert_uuid.

# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
# library(aws.s3)
library(lubridate)
library(doParallel)
library(foreach)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
 
# aws.s3::get_bucket(output.loc) # forbidden 403, as aws.s3 relies on credentials, which we are not using. see: aws.signature::locate_credentials()

output.loc <- "~/tempout"
localdir <- "/home/daniel/workingdata/" # full path for readOGR

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))

# Query parameters
# states with available EDT data for model testing
states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"),
                  stringsAsFactors = F)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling by State ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:4, width = 2, flag = "0"), sep="-")
)
yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

starttime <- Sys.time()

# Parallizing this will not help at this point; Redshift only running on one node.
for(i in states){ # i = 1
  
  # read data by state, across all query months for processing
  
  alert_query <- paste0("SELECT * FROM alert WHERE state='", i,
                        "' AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
                        " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
  
  results <- dbGetQuery(conn, alert_query) 
  
  cat(format(nrow(results), big.mark = ","), "observations in", i, "\n")
  timediff <- Sys.time() - starttime
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to query", i, "\n\n")
  
  fn = paste0(i, "_Raw_events_to_", yearmonths[length(yearmonths)],".RData")
  
  save("results", file = file.path(output.loc, fn))
  
  # Copy to S3
  system(paste("aws s3 cp",
               file.path(output.loc, fn),
               file.path(teambucket, i, fn)))
  
  timediff <- Sys.time() - starttime
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to save", i, "\n\n")
  
}

 # ~ 1.6 h for MD, CT, UT, and VA. File size seems too small for non-MD states... VA especially. Check date range of actual data. May simply be consequence of rectangular 'state' definition by Waze.

# Clip to state boundaries ----
  # ~ 2 h for these four states
  # Depends on *_Raw_events_to_lastyearmonth.RData being in localdir,
  # and *_buffered.shp (and associated files) being in localdir/census.
  # Will save *_Buffered_Clipped_events_to_lasyearmonth.RData in localdir and to S3
cliptime <- Sys.time()
source(file.path(codeloc, "datacleaning/Waze_clip.R"))
td <- Sys.time()-cliptime
cat("\n\n", round(td, 2), attr(td, "units"), "to complete all Waze_clip steps \n\n")


# Reduce to single UUID by state ----

# First, loop over states again. Then parallel process the months. 
# MD: > 11 Gb full spatial points data frame, saved after converting back to data.frame and removing unused columns, only 8.4 Gb. 

starttime <- Sys.time()
  
for(i in states){ # i = "UT"
  
  # Pick out the right time zone for this state
  use.tz <- tzs$tz[tzs$states == i]
  
  # Get the data from the S3 bucket if needed (to do), saving to workingdata
  load(file.path("~/workingdata", paste0(i, "_Buffered_Clipped_events_to_", yearmonths[length(yearmonths)],".RData")))
  
  # Convert values to numeric
  numconv <- c("report_rating", "confidence", "reliability", "pub_millis", "location_lon", "location_lat", "magvar")
  
  # Drop from spatial data frame back to data frame for easier aggregation. Will need to re-project for linking. Also makes object size smaller.
  d <- d@data
  d[numconv] <- apply(d[numconv], 2, function(x) as.numeric(x))
  
  # Year-month for looping
  ym <- format(d$pub_utc_timestamp, "%Y-%m")
  
  # Removed unneeded variables to save ram
  dropvars = c("report_description", "pub_utc_epoch_week","jam_uuid", "num_thumbsup")
  d <- d[,!names(d) %in% dropvars]

  # Manual 'loop' for MD. Have to re-make ym and also re-load for each half. Work on this to optimize when dataframe d is some % of available RAM 
  # d <- d[ym %in% yearmonths[8:13],];  ym <- format(d$pub_utc_timestamp, "%Y-%m"); gc()
  
  # Set up cluster. 
  avail.cores <- parallel::detectCores()
  if(avail.cores > length(yearmonths)) avail.cores = length(yearmonths) # use only cores necessary
  cl <- makeCluster(avail.cores) 
  registerDoParallel(cl)
  
# Start parallel loop over yearmonths within state ----
foreach(mo = unique(ym), .packages = c("dplyr", "tidyr")) %dopar% {
    # mo = "2018-04"
    dx <- d[ym == mo,]
    
    # d.test = dx[1:5000,]
    
    ll <- dx %>%
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
    ll2 <-  dx %>% 
      group_by(alert_uuid) %>%
      filter(pub_millis == max(pub_millis, na.rm = T)) %>%
      summarize(
        last.reportRating = median(report_rating),
        last.confidence = median(confidence),
        last.reliability = median(reliability),
        magvar = median(magvar, na.rm = T)
      )
    
    # part 3: get most frequent values for city, type, road_type, sub_type, and street. Fastest dplyr method is with slice(whic.max(n))

    ll3 <- dx %>%
      group_by(alert_uuid) %>%
      count(city, alert_type, sub_type, street, road_type) %>%
      slice(which.max(n)) 
    
    # join these two three tables
    lx <- full_join(ll, ll2, by = 'alert_uuid')
    lx <- full_join(lx, ll3, by = 'alert_uuid')
    
    # Format and output  
    time <- as.POSIXct(lx$pubMillis/1000, origin = "1970-01-01", tz= use.tz) 
    last.pull.time <- as.POSIXct(lx$last.time/1000, origin = "1970-01-01", tz= use.tz) 
    
    mb <- data.frame(lx, time, last.pull.time)
    
    filenamesave = paste(i, "_",
                         mo,
                         sep = "")
     
    save(list = "mb", 
         file = file.path(output.loc, paste0(filenamesave, ".RData")))
    
    # Space after cp and before s3 for proper formatting
    system(paste("aws s3 cp", 
                 file.path(output.loc, paste0(filenamesave, ".RData")),
                 file.path(teambucket, i, paste0(filenamesave, ".RData"))))
    
    timediff <- Sys.time() - starttime
    
    cat(round(timediff, 2), attr(timediff, "units"), "elapsed to save", i, "\n\n")
    
    cat(format(nrow(mb), big.mark = ","), "observations in aggregated data \n")
  } # end month foreach loop
  
  rm(d, ym)
  stopCluster(cl); gc()
  
  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", 
    timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
} # end state loop


