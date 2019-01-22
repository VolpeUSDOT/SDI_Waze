# Precursor to Grid_aggregation, follows Hex_UA_overlay_SDC


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
rm(list = ls())
library(tidyverse)
library(lubridate)
library(utils)
library(doParallel)
library(foreach)

#Set parameters for data to process
grids = c("TN_01dd_fishnet",
          "TN_1sqmile_hexagons")

codeloc <- "~/SDI_Waze" 

home.loc <- getwd()
user <- if(length(grep("@securedatacommons.com", home.loc)) > 0) {
  paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
} else {
  paste0( "/home/", system("whoami", intern = TRUE))
} # find the user directory to use
localdir <- paste0(user, "/workingdata/") # full path for readOGR

wazemonthdir <- "~/workingdata/TN/Overlay" # contains the merged.waze.tn.YYYY-mm_<state>.RData files
temp.outputdir = "~/agg_out" # Will contain the WazeHexTimeList_YYYY-mm_grids_<state>.RData files # Which code generated this? Jessie does not have this file.

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
  
source(file.path(codeloc, "utility/wazefunctions.R")) 

setwd(wazemonthdir)


# <><><><><><><><><><><><><><><><><><><><>

# start grid loop ----
for(g in grids){ # g = grids[1]

  # Loop through months of available merged data for this state
  mergefiles <- dir(wazemonthdir)[grep("^merged.waze.tn", dir(wazemonthdir))]
  gridmergefiles <- mergefiles[grep(g, mergefiles)]
 
  avail.months = substr(unlist(lapply(strsplit(gridmergefiles, "_"), function(x) x[[4]])),
                        1, 7)
  
  # Look for already completed months and skip those
  tlfiles <- dir(temp.outputdir)[grep("WazeHexTimeList_", dir(temp.outputdir))]
  g.tlfiles <- tlfiles[grep(g, tlfiles)]
  done.months <- unlist(lapply(strsplit(g.tlfiles, "_"), function(x) x[[2]])) 

  todo.months = avail.months[!avail.months %in% done.months] #sort(avail.months)[c(1:9)]

  
  starttime <- Sys.time()
  
  cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
  registerDoParallel(cl)
  
  writeLines(c(""), paste(g, "log.txt", sep = "_"))    
  
  foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% { # j="2017-04"

    sink(paste(g, "log.txt", sep = "_"), append=TRUE)
    
    cat(paste(Sys.time()), g, j, "\n")                                                           
                                                             
    
    load(file.path(wazemonthdir, paste0("merged.waze.tn.", g,"_", j, ".RData"))) # includes both waze (link.waze.tn) and TN crash (crash.df) data, with grid for central and neighboring cells
    
    # format(object.size(link.waze.tn), "Mb"); format(object.size(crash.df), "Mb")
    # TN date, Waze time all now are POSIXct, with correct time zone. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.

    ##############
    # Make data frame of all Grid IDs by day of year and time of day in each month of data (subset to all grid IDs with Waze OR EDT data)
    GridIDall <- unique(c(unique(as.character(link.waze.tn$GRID_ID)), unique(as.character(link.waze.tn$GRID_ID.TN))))
    # x <- data.frame(length(unique(as.character(link.waze.tn$GRID_ID))), length(unique(as.character(link.waze.tn$GRID_ID.TN))), length(GridIDall), length(unique(GridIDall)))
    # 837, 698, 1535, 922 # using "2017-04" as an example, apparently, we have some duplication in GridIDall
    
    # Date/time for TN crash only events or TN/Waze matching events are stored as 'date', while Date/time for Waze only events are stored as 'time'.
    year.month.w <- format(link.waze.tn$time, "%Y-%m")
    year.month.t <- format(link.waze.tn$date, "%Y-%m")
    year.month <- year.month.w
    year.month[is.na(year.month)] <- year.month.t[is.na(year.month)]
    
    link.waze.tn <- link.waze.tn[year.month == j,]
    
    month.days.w  <- unique(as.numeric(format(link.waze.tn$time, "%d"))) # Waze event date/time
    month.days.t  <- unique(as.numeric(format(link.waze.tn$date, "%d"))) # TN crash date/time
    month.days = unique(c(month.days.w, month.days.t))
    
    lastday = max(month.days[!is.na(month.days)])
    
    Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = 'America/Chicago'), 
                      to = as.POSIXct(paste0(j,"-", lastday, " 24:00"), tz = 'America/New_York'),
                      by = "hour")
    
    GridIDTime <- expand.grid(Month.hour, GridIDall)
    names(GridIDTime) <- c("GridDayHour", "GRID_ID")
  
    # Temporally matching
    # Match between the first reported time and last pull time of the Waze event. 
    StartTime <- Sys.time()
    t.min = min(GridIDTime$GridDayHour)
    t.max = max(GridIDTime$GridDayHour)
    i = t.min
    
    Waze.hex.time.all <- vector()
    counter = 1
    while(i+3600 <= t.max){
      ti.GridIDTime = filter(GridIDTime, GridDayHour == i)
      ti.link.waze.tn = link.waze.tn %>% filter(time >= i & time <= i+3600 | last.pull.time >= i & last.pull.time <=i+3600)
      ti.link.waze.tn.t = link.waze.tn %>% filter(date >= i & date <= i+3600)
      
      ti.Waze.hex <- inner_join(ti.GridIDTime, ti.link.waze.tn, by = "GRID_ID") #Use left_join to get zeros if no match  
      ti.Waze.hex.t <- inner_join(ti.GridIDTime, ti.link.waze.tn.t, by = "GRID_ID") # Same, for TN only crashes
      
      Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex)
      Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex.t)
      
      i=i+3600
      if(counter %% 3600*24 == 0) cat(paste(i, "\n"))
     } # end loop
    
    EndTime <- Sys.time() - StartTime
    cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
    
    Waze.hex.time <- filter(Waze.hex.time.all, !is.na(GRID_ID))
    
    #Save list of Grid cells and time windows with EDT or Waze data  
    fn = paste0("WazeHexTimeList_", j,"_", g, "TN.RData")
    
    save(list="Waze.hex.time", file = file.path(temp.outputdir, fn))
    
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, fn),
                 file.path(teambucket, "TN", fn)))
    
    cat("Completed", j, "\n")
    } # End SpaceTimeGrid loop ----
  
  stopCluster(cl); gc()
  
} # end grid loop
