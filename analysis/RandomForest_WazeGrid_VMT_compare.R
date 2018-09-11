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

# <><><><><>
state = 'MD'  #'UT' # Sets the state. UT, VA, MD are all options.
# <><><><><>

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

# rename data files by month. For each month, prep time and response variables
# See prep.hex() in wazefunctions.R for details.
for(mo in do.months){
  prep.hex(paste0("WazeTimeEdtHexAll_", mo, "_", HEXSIZE, "mi_", state,".RData"), state = state, month = mo)
}

# Plot to check grid IDs. Requires shapefiles to be present in ~/workingdata/Hex, download from S3 if not there.
# This was useful when testing different grid sizes, to make sure everything was matching correctly.
CHECKPLOT = F
if(CHECKPLOT){
  grid_shp <- rgdal::readOGR(file.path(localdir, "Hex"), paste0(state, "_hexagons_1mi_neighbors"))
  
  w.g <- match(w.2017_04$GRID_ID, grid_shp$GRID_ID)
  w.g <- w.g[!is.na(w.g)]
  gs <- grid_shp[w.g,]
  
  plot(gs, col = "red")
  rm(w.g, gs, grid_shp)
}

# Update this later after getting supplemental data ready
# Add FARS, AADT, HPMS, jobs
 na.action = "fill0"
# for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
#   append.hex(hexname = w, data.to.add = "FARS_MD_2012_2016_sum_annual", na.action = na.action)
    append.hex2(hexname = w, data.to.add = paste0("FARS_", state, "_2012_2016_sum_annual"), state = state, na.action = na.action)
    
    # VMT
    append.hex2(hexname = w, data.to.add = paste0(state, "_max_aadt_by_grid_fc_urban_vmt_factored"), state = state, na.action = na.action)
    
    append.hex2(hexname = w, data.to.add = "hexagons_1mi_routes_AADT_total_sum", na.action = na.action)

 #   append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = na.action)
#   append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum", na.action = na.action)
#   append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum", na.action = na.action)
#   }


# Bind all months together
w.allmonths.named <- ls()[grep("^w.", ls())]

w.allmonths <- vector()
for(i in w.allmonths.named){
  w.allmonths <- rbind(w.allmonths, get(i))
}

# format(object.size(w.allmonths), "Gb")

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage below max if on r4.4xlarge instance

# Use this to set number of decision trees to use, and key RF parameters. mtry is especially important, should consider tuning this with caret package
# For now use same parameters for all models for comparision; tune parameters after models are selected
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"


# Compare MD AADT vs VMT ----

# 21 18 base + Add road class, AADT only
# 23 Add all together

# 60 base 30 - neighbors
# 61 base 30 - AADT
# 62 base 18 + VMT
# 63 base 30 + VMT  

starttime = Sys.time()

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

modelno = "18"

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "MatchEDT_buffer_Acc", 
                               # thin.dat = 0.01,
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
  } else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
}


save("keyoutputs", file = paste0("Output_to_", modelno))

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)




# 30 Add all (but not Waze event subtypes or sub-subtypes)
modelno = "30"
omits = c(alwaysomit, alert_subtypes
          #"wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          #c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          #grep("RAC", names(w.allmonths), value = T), # Jobs residential
          #grep("MagVar", names(w.allmonths), value = T), # direction of travel
          #grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchEDT_buffer_Acc",  
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths,
                                        omits, response.var = "MatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

# 60 


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")


