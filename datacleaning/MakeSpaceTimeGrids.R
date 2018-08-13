# Aggregation of Waze and EDT by grid cell
# Goal: create a gridded data set where grid cell contain the count of 
# Start from UrbanArea_overlay.R


# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(tidyverse)
library(lubridate)
library(utils)
library(doParallel)
library(foreach)

#Set parameters for data to process
HEXSIZE = c("1", "4", "05")[3] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
ONLOCAL = F

if(ONLOCAL){
##Set file paths - remove for cloud pipeline
#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                    "WazeEDT Agg",HEXSIZE,"mile Rdata Input")
} else {
  
  codedir <- "~/SDI_Waze" 
  wazemonthdir <- "~/workingdata/Overlay"
  wazedir <- "~/workingdata"
  volpewazedir <- "~/workingdata"
  outputdir <- file.path("~/workingdata",
                      paste("WazeEDT_Agg",HEXSIZE,"mile_Rdata_Input", sep = "_"))
  
}


# #Sudderth drive
# #NEED TO CONNECT TO VPN AND CLICK ON "S" DRIVE IN FILE EXPLORER FIRST
# codedir <- "~/GitHub/SDI_Waze"  
# wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
# wazedir <- "S:/SDI Pilot Projects/Waze/"
# volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
# outputdir <- paste0("S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
#                     "WazeEDT Agg",HEXSIZE,"mile Rdata Input")

source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function
setwd(wazedir)

# Loop through months of available merged data
avail.months = unique(substr(dir(wazemonthdir)[grep("^merged.waze.edt", dir(wazemonthdir))], 
                      start = 17,
                      stop = 23))

todo.months = avail.months#[c(2:7)]

if(ONLOCAL) { temp.outputdir = tempdir() # for temporary storage 
} else {
  temp.outputdir = "~/agg_out"
}
starttime <- Sys.time()

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

writeLines(c(""), "log.txt")    

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils")) %dopar% {
  
  sink("log.txt", append=TRUE)
  
  cat(paste(Sys.time()), j, "\n")                                                           
                                                           
# for(j in todo.months){ # j="04"
  
  load(file.path(wazemonthdir, paste0("merged.waze.edt.", j,"_", HEXSIZE, "mi","_MD.RData"))) # includes both waze (link.waze.edt) and edt (edt.df) data, with grid for central and neighboring cells
  
  # format(object.size(link.waze.edt), "Mb"); format(object.size(edt.df), "Mb")
  # EDT time needs to be POSIXct, not POSIXlt. ct: seconds since beginning of 1970 in UTC. lt is a list of vectors representing seconds, min, hours, day, year. ct is better for analysis, while lt is more human-readable.
  link.waze.edt$CrashDate_Local <- as.POSIXct(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  edt.df$CrashDate_Local <- as.POSIXct(edt.df$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  
  ##############
  # Make data frame of all Grid IDs by day of year and time of day in each month of data (subset to all grid IDs with Waze OR EDT data)
  GridIDall <- c(unique(as.character(link.waze.edt$GRID_ID)), unique(as.character(link.waze.edt$GRID_ID.edt)))
  
  month.days <- unique(as.numeric(format(link.waze.edt$last.pull.time, "%d")))
  lastday = max(month.days[!is.na(month.days)])
  
  Month.hour <- seq(from = as.POSIXct(paste0("2017-", j,"-01 0:00"), tz = "America/New_York"), 
                    to = as.POSIXct(paste0("2017-", j,"-", lastday, " 23:00"), tz = "America/New_York"),
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
  save(list="Waze.hex.time", file = paste(temp.outputdir, "/WazeHexTimeList_", j,"_", HEXSIZE, "mi",".RData",sep=""))
  
  cat("Completed", j, "\n")
  } # End SpaceTimeGrid loop ----

stopCluster(cl)


if(ONLOCAL) movefiles(dir(temp.outputdir)[grep("Hex", dir(temp.outputdir))], temp.outputdir, outputdir)


  
