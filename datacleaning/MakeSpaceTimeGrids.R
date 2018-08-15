# Precursor to Grid_aggregation, follows Hex_UA_overlay_SDC


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(tidyverse)
library(lubridate)
library(utils)
library(doParallel)
library(foreach)

#Set parameters for data to process
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

codedir <- "~/SDI_Waze" 

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata/") # full path for readOGR

wazemonthdir <- "~/workingdata/Overlay" # contains the merged.waze.edt.YYYY-mm_<state>.RData files
temp.outputdir = "~/agg_out" # Will contain the WazeHexTimeList_YYYY-mm_HEXSIZE_<state>.RData files

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
  
source(file.path(codeloc, "utility/wazefunctions.R")) 

states = c("CT", "UT")#, "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                    "US/Mountain"#,
                    #"US/Eastern",
                    #"US/Eastern"
                    ),
                  stringsAsFactors = F)

setwd(wazemonthdir)


# <><><><><><><><><><><><><><><><><><><><>

# start state loop ----
for(state in states){ # state = "CT"

  # Loop through months of available merged data for this state
  mergefiles <- dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))]
  statemergefiles <- mergefiles[grep(paste0(state, ".RData"), mergefiles)]
 
  avail.months = unique(substr(statemergefiles, 
                               start = 17,
                               stop = 23))
  
  todo.months = sort(avail.months)[c(1:9)]

  use.tz <- tzs$tz[tzs$states == state]
  
  starttime <- Sys.time()
  
  cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
  registerDoParallel(cl)
  
  writeLines(c(""), "log.txt")    
  
  foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% { # j="2017-04"

    sink("log.txt", append=TRUE)
    
    cat(paste(Sys.time()), j, "\n")                                                           
                                                             
    
    load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_", state, ".RData"))) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
    
    # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
    # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
    link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = use.tz)
    edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = use.tz)
    
    ##############
    # Make data frame of all Grid IDs by day of year and time of day in each month of data (subset to all grid IDs with Waze OR EDT data)
    GridIDall <- c(unique(as.character(link.waze.edt$GRID_ID)), unique(as.character(link.waze.edt$GRID_ID.edt)))
    
    # Eliminate any rows which are not for this month -- may happen if an event began the last minute of the last day of previous month.
    
    link.waze.edt <- link.waze.edt[!is.na(link.waze.edt$last.pull.time),]
    
    year.month <- format(link.waze.edt$last.pull.time, "%Y-%m")
    
    link.waze.edt <- link.waze.edt[year.month == j,]
    
    month.days <- unique(as.numeric(format(link.waze.edt$last.pull.time, "%d")))
    
    lastday = max(month.days[!is.na(month.days)])
    
    Month.hour <- seq(from = as.POSIXct(paste0(j,"-01 0:00"), tz = use.tz), 
                      to = as.POSIXct(paste0(j,"-", lastday, " 23:00"), tz = use.tz),
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
      ti.link.waze.edt = filter(link.waze.edt, time >= i & time <= i+3600 | last.pull.time >= i & last.pull.time <=i+3600)
    
       ti.Waze.hex <- inner_join(ti.GridIDTime, ti.link.waze.edt, by = "GRID_ID") #Use left_join to get zeros if no match  
       Waze.hex.time.all <- rbind(Waze.hex.time.all, ti.Waze.hex)
  
      i=i+3600
      if(counter %% 3600*24 == 0) cat(paste(i, "\n"))
     } # end loop
    
    EndTime <- Sys.time() - StartTime
    cat(round(EndTime, 2), attr(EndTime, "units"), "\n")
    
    Waze.hex.time <- filter(Waze.hex.time.all, !is.na(GRID_ID))
    
    #Save list of Grid cells and time windows with EDT or Waze data  
    fn = paste0("WazeHexTimeList_", j,"_", HEXSIZE, "mi_", state, ".RData")
    
    save(list="Waze.hex.time", file = paste(temp.outputdir, fn))
    
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(temp.outputdir, fn),
                 file.path(teambucket, state, fn)))
    
    cat("Completed", j, "\n")
    } # End SpaceTimeGrid loop ----
  
  stopCluster(cl)
  
} # end state loop
