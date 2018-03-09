# Aggregates Waze files to monthly sets with unique UUIDs. 
# Original 'ReduceWaze2.R' for working with daily .csv files, now directly reading from Redshift in SDC 

# setup ----

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(aws.s3)

# output.loc <- "prod-sdc-sdi-911061262852-us-east-1-bucket"
# 
# get_bucket(output.loc) # forbidden 403. 

output.loc <- "~/tempout"

# read functionss
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/wazefunctions.R'))

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling Monthly ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Compiling daily into monthly files takes ~ 16 minutes per month
# Consider speeding up with library(foreach), using %dopar%

# create a temporary directory on the C: drive of the local machine to save files
outdir <- tempdir()

# read files
wazefiles <- dir()

# filter: only .RData
wazefiles = wazefiles[grep("RData$", wazefiles)]

# find available months (was only April initially)
months = unique(substr(wazefiles, 10, 11))

starttime <- Sys.time()

# Read in each day, rbind together, 
# get dates in correct format, output to csv and RData

for(i in months){ # i = "04"
  wf = wazefiles[substr(wazefiles, 10, 11) == i]

  monthbind <- vector()
  
  # Day loop:
  for(j in 1:length(wf)){ # j = 1
    load(wf[j]) # each RData has a dataframe ll for that day
    monthbind <- rbind(monthbind, ll)
    cat(". ")
  }
  
  monthbind$date <- as.POSIXct(paste(
    substr(monthbind$last.file, 5, 14), " ", substr(monthbind$last.file, 16, 17), ":", 
    substr(monthbind$last.file, 19,20), ":", substr(monthbind$last.file, 22, 23), sep = ""))
  

  # Aggregate again (uuid can span days). This can be very slow. 
  d <- as.tibble(monthbind)
  ll <- d %>%
    group_by(uuid) %>%
    summarise(
      median.reportRating = median(median.reportRating),
      median.confidence = median(median.confidence),
      median.reliability = median(median.reliability),
      pubMillis = min(pubMillis),
      lon = median(lon),
      lat = median(lat),
      first.file = sort(first.file, decreasing = F)[1],
      last.file = sort(last.file, decreasing = T)[1],
      nrecord = sum(nrecord) # instead of n(), because now aggregating the aggregated version 
    )
  # now add the following for the *most recent* row within a uuid, using filter(date == max(date)): city, report rating, confidence, and reliability, type, road type, directoin as max(magvar), subtype and street.
  # Using ModeString function here in case there are multiple uuid at the maximum date (should not be)
  ll2 <-  d %>% 
    group_by(uuid) %>%
    filter(as.numeric(date) == max(as.numeric(date), na.rm=T)) %>%
    summarize(
      city = ModeString(city),
      last.reportRating = median(last.reportRating),
      last.confidence = median(last.confidence),
      last.reliability = median(last.reliability),
      type = ModeString(type),
      roadType = ModeString(as.character(roadType)),
      magvar = max(magvar, na.rm=T),
      subtype = ModeString(subtype),
      street = ModeString(street)
    )
  
  # join these two sets and sort columns 
  lx <- full_join(ll, ll2, by = 'uuid')
  ll <- lx # keep naming convention for compatibility with existing code
  
  
  # Format and output  
  time <- as.POSIXct(ll$pubMillis/1000, origin = "1970-01-01", tz="America/New_York") # Time zone will need to be correctly configured for other States.
  
  # get initial and last pull times from filename
  first.pull.time <- gettime(ll$first.file)
  last.pull.time <- gettime(ll$last.file)

  mb <- data.frame(ll, time, first.pull.time, last.pull.time)
  
  filenamesave = paste("MD__",
                   format(first.pull.time[1], "%Y"),
                   "-",
                   i,
                   sep = "")
  
  write.csv(mb,
            file = file.path(outdir, paste0(filenamesave, ".csv")),
            row.names = F)
  
  save(list = "mb", 
       file = file.path(outdir, paste0(filenamesave, ".RData")))

  timediff <- round(Sys.time()-starttime, 2)
  cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
  
  
  } # end month bind loop

# after completed, move these aggregated files from C: to shared drive
wazedir <- file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD")

# Move either RData or csv 
filelist <- dir(outdir)[grep("[RData$csv$]", dir(outdir))]

movefiles(filelist, outdir, wazedir)
