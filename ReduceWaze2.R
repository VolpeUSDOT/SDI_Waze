# Aggregates daily Waze files 
# This is an edited version of Lia's original 'ReduceWaze2.R' from 
# W:\SDI Pilot Projects\Waze\Working Documents\R
# edited 11/28/2017

# setup ----

library(tidyverse)

if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} # change this to match the mapped network drive on your machine (Z: in original)
if(length(grep("sudderth", getwd())) > 0) {mappeddrive = "S:"} 

setwd(file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze"))

# Helper functions ----

# Function to return the most frequent value for character vectors.
# Breaks ties by taking the first value
ModeString <- function(x) {
  ux <- unique(x)
  
  # One unique value: return that value
  if(length(ux)==1) { 
    return(ux)
  } else {
    
    # Multiple values, no duplicates: return first one
    if(!anyDuplicated(x)) {
      return(ux[1])
    } else {
      
      # Multiple values, one category more frequent: return first most frequent
      tbl <-   tabulate(match(x, ux))
      return(ux[tbl==max(tbl)][1])
    }
  }
}

# Function for extracting time from file names
gettime <- function(x, tz = "America/New_York"){
  d <- substr(x, 5, 14)
  t <- substr(x, 16, 23)
  dt <- strptime(paste(d, t), "%Y-%m-%d %H-%M-%S", tz = tz)
}

# moving files from a temporary directory on local machine to shared drive. 
# Files are removed from the local machine by this process.
movefiles <- function(filelist, temp = outdir, wazedir){
  for(i in filelist){
      # Fix path separators for Windows / R 
      temp <- gsub("\\\\", "/", temp)
      temp <- gsub("C:/U", "C://U", temp)
      
      # Encase the destination path in quotes, because of spaces in path name
      system(paste0("mv ", file.path(temp, i), ' \"', file.path(wazedir, i), '\"'))
  
      }
    }

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Make aggregated daily files ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# read file names
wazefiles <- dir()[grep("csv", dir())]

# Initially did April only, now do all others.
wazefiles = wazefiles[substr(wazefiles, 10, 11) != "04"]

# create a temporary directory on the C: drive of the local machine to save files
outdir <- tempdir()

#for each file
j = 1 # 

starttime <- Sys.time()

while (j <= length(wazefiles)){
  
  bydate <- read.csv(file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze/", wazefiles[j]))

  #extract date from file name
  bydate$date <- as.POSIXct(paste(substr(bydate$filename, 5, 14), " ", substr(bydate$filename, 16, 17), ":", substr(bydate$filename, 19,20), ":", substr(bydate$filename, 22, 23), sep = ""))
  
  # drop un-useful columns
  dropcols <- c("X", "country")
  bydate <- bydate[is.na(match(names(bydate), dropcols))]
  
 
  #aggregate by uuid. Now using dplyr.
  # First step: median values for report rating, confidence, and reliability. Earliest report rating as min(pubMillis). Lat long as median of all reported values. File names for first and last pull date. Sum of records 
  d <- as.tibble(bydate)
  ll <- d %>%
         group_by(uuid) %>%
         summarise(
                   median.reportRating = median(reportRating),
                   median.confidence = median(confidence),
                   median.reliability = median(reliability),
                   pubMillis = min(pubMillis),
                   lon = median(location.x),
                   lat = median(location.y),
                   first.file = sort(filename, decreasing = F)[1],
                   last.file = sort(filename, decreasing = T)[1],
                   nrecord = n()
                   )
    # now add the following for the *most recent* row within a uuid, using filter(date == max(date)): city, report rating, confidence, and reliability, type, road type, directoin as max(magvar), subtype and street.
    # Using ModeString function here in case there are multiple uuid at the maximum date (should not be)
    ll2 <-  d %>% 
        group_by(uuid) %>%
        filter(date == max(date)) %>%
        summarize(
          city = ModeString(city),
          last.reportRating = median(reportRating),
          last.confidence = median(confidence),
          last.reliability = median(reliability),
          type = ModeString(type),
          roadType = ModeString(as.character(roadType)),
          magvar = max(magvar),
          subtype = ModeString(subtype),
          street = ModeString(street)
          )
  
    # join these two sets and sort columns 
    lx <- full_join(ll, ll2, by = 'uuid')
    ll <- lx # keep naming convention for compatibility with existing code
    
  # inspect a particular uuid in original to test:
  # bydate[bydate$uuid=="6407a977-9d52-380e-b545-b4ef58e70337",]
  # test: xx <- tapply(bydate$roadType, bydate$uuid, ModeString); which(sapply(xx, length) > 1)
  
  # write this day's data to RData output 
  filenamesave <- sub("csv", "RData", wazefiles[j])
  
  save(list = "ll", 
       file = file.path(outdir, filenamesave))

  timediff <- round(Sys.time()-starttime, 2)
  cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")

  j = j+1
  }


# after completed, move these aggregated files from C: to shared drive
wazedir <- file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/day_MD")
filelist <- dir(outdir)[grep("RData$", dir(outdir))]

movefiles(filelist, outdir, wazedir)

# Compiling daily files takes ~ 2-4 min per day; this is 7-14 h of run time for the 7 months of data.

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling Monthly ----
# <><><><><><><><><><><><><><><><><><><><><><><><>


setwd(file.path(mappeddrive, "/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/day_MD"))

# read files
wazefiles <- dir()

# filter: only .RData
wazefiles = wazefiles[grep("RData$", wazefiles)]

# find available months (only April initially)
months = unique(substr(wazefiles, 10, 11))

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

  # Aggregate again (uuid can span days). This can be very slow. 
  d <- as.tibble(monthbind)
  ll <- d %>%
    group_by(uuid) %>%
    summarise(
      city = ModeString(city),
      reportRating = median(reportRating),
      confidence = median(confidence),
      reliability = median(reliability),
      type = ModeString(type),
      roadType = ModeString(as.character(roadType)),
      magvar = max(magvar),
      subtype = ModeString(subtype),
      street = ModeString(street),
      pubMillis = min(pubMillis),
      lon = median(lon),
      lat = median(lat),
      first.file = sort(first.file, decreasing = F)[1],
      last.file = sort(last.file, decreasing = T)[1],
      nrecord = sum(nrecord) # instead of n(), because now aggregating the aggregated version 
    )
  
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
            file = paste0("../month_MD/", filenamesave, ".csv"),
            row.names = F)
  
  save(list = "mb", 
       file = paste0("../month_MD/", filenamesave, ".RData"))
  
  }