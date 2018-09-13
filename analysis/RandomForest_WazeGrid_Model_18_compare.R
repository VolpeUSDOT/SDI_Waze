# Running full set of random forest models
# https://github.com/VolpeUSDOT/SDI_Waze/wiki/Phase-2-Models-to-Test


# Setup ---- 
rm(list=ls()) # Start fresh
library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

codeloc <- "~/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

# Set grid size:
HEXSIZE = 1 

# Manually setting months to run here; could also scan S3 for months available for this state
do.months = paste("2017", c("04","05","06","07","08","09"), sep="-")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

outputdir <- file.path(localdir, "Random_Forest_Output")

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# check if already completed transfer of necessary supplemental data on this instance
if(length(dir(localdir)[grep("aadt_by_grid", dir(file.path(localdir, 'AADT')))]) == 0){

  source(file.path(codeloc, "utility/Workstation_setup.R"))
  # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
  system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
}

# View the files available in S3 for this state: system(paste0('aws s3 ls ', teambucket, '/', state, '/'))

# Loop over months to prepare for comparison

# <><><><><>
states = c('CT', 'MD')  #'UT' # Sets the state. UT, VA, MD are all options.
# <><><><><>

for(state in states){
  cat("Compiling input data for", state, "\n")
  # rename data files by month. For each month, prep time and response variables
  # See prep.hex() in wazefunctions.R for details.
  for(mo in do.months){
    prep.hex(paste0("WazeTimeEdtHexAll_", mo, "_", HEXSIZE, "mi_", state,".RData"), state = state, month = mo)
  }
  
  monthfiles = paste("w", do.months, sep=".")
  monthfiles = sub("-", "_", monthfiles)
  
  # Bind all months together, and remove monthly files to save memory
  
  w.allmonths <- vector()
  for(i in monthfiles){
    w.allmonths <- rbind(w.allmonths, get(i))
  }
  rm(list = monthfiles)
  
  assign(paste(state, "w.allmonths", sep="_"), w.allmonths)
  rm(w.allmonths)
  
} # End state data prep for comparison. Now have <state>_w.allmonths data frames for each state

# format(object.size(w.allmonths), "Gb")

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage below max if on r4.4xlarge instance

# Use this to set number of decision trees to use, and key RF parameters. mtry is especially important, should consider tuning this with caret package
# For now use same parameters for all models for comparision; tune parameters after models are selected
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(MD_w.allmonths), value = T), "day", "hextime", "year", "weekday", "vmt_time",
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(MD_w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"


# Compare MD and CT Model 18 ----

# Run MD Model 18

starttime = Sys.time()

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          grep("SUM_MAX_AADT", names(w.allmonths), value = T), # AADT
          grep("HOURLY_MAX_AADT", names(w.allmonths), value = T), # Hourly VMT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

modelno = "18"

keyoutputs[[paste("MD", modelno, sep="_")]] = do.rf(train.dat = MD_w.allmonths, 
                                omits, response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  

# Run CT Model 18

# Run MD Model 18 on CT data

# Run CT Model 18 on MD data

# Save outputs to S3
fn = paste0(state, "_VMT_Output_to_", modelno, ".RData")

save("keyoutputs", file = fn)

system(paste("aws s3 cp",
             file.path(localdir, fn),
             file.path(teambucket, state, fn)))

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")