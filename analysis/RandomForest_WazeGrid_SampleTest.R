# Running full set of random forest models - Here testing different sampling, 70/30 by day and by week
# https://github.com/VolpeUSDOT/SDI_Waze/wiki/Models-to-test

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(aws.s3)
library(tidyverse)
library(rgdal)
# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal", "foreach", "doParallel"), dep = T)

codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
do.months = c("04","05","06","07","08","09")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

aws.signature::use_credentials()
waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)
# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# check if already complted this transfer on this instance
if(length(dir(localdir)[grep("shapefiles_funClass", dir(localdir))]) == 0){

  s3transfer = paste("aws s3 cp s3://ata-waze/MD_hexagon_shapefiles", localdir, "--recursive --include '*'")
  system(s3transfer)
  
  for(dd in c("shapefiles_funClass.zip", "shapefiles_rac.zip", "shapefile_wac.zip", "shapefiles_AADT.zip", "shapefiles_FARS.zip")){
    uz <- paste("unzip", file.path(localdir, dd))
    system(uz)
  }
  # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
  system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
}

# rename data files by month. For each month, prep time and response variables
# See prep.hex() in wazefunctions.R for details.
for(mo in do.months){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

# Plot to check grid IDs
CHECKPLOT = F
if(CHECKPLOT){
  grid_shp <- rgdal::readOGR(localdir, "MD_hexagons_1mi_newExtent_newGRIDID")
  
  w.g <- match(w.04$GRID_ID, grid_shp$GRID_ID)
  w.g <- w.g[!is.na(w.g)]
  gs <- grid_shp[w.g,]
  
  plot(gs, col = "red")
}

# Add FARS, AADT, HPMS, jobs
na.action = "fill0"
for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "FARS_MD_2012_2016_sum_annual", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_AADT_total_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum", na.action = na.action)
  }
  
w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage below max if on r4.4xlarge instance

rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"

# 23 Add all together
modelno = "23_original"

omits = c(alwaysomit,
#          "wx",
#          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
#          grep("F_SYSTEM", names(w.04), value = T), # road class
#          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
#          grep("WAC", names(w.04), value = T), # Jobs workplace
#          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
modelno = "23_day"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "day",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
modelno = "23_week"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "week",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

  
# 30 Add all (but not Waze event subtypes or sub-subtypes)
modelno = "30_original"
omits = c(alwaysomit, alert_subtypes
          #"wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.04), value = T), # road class
          #c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #grep("WAC", names(w.04), value = T), # Jobs workplace
          #grep("RAC", names(w.04), value = T), # Jobs residential
          #grep("MagVar", names(w.04), value = T), # direction of travel
          #grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))

modelno = "30_day"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "day",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
modelno = "30_week"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "week",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
