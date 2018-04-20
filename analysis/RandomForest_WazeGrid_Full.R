# Running full set of random forest models
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

inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
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

avail.cores = parallel::detectCores()
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 8, maxnodes = NULL, nodesize = 50)
keyoutputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder",
                   "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",            "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"

# Models 01-03 ----
REDO01_03 = FALSE

if(REDO01_03){
# Re-running here to check consistency with previous work
omits = c(alwaysomit, alert_types,
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          "wx", 
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

modelno = "01"

keyoutputs[[modelno]] = do.rf(train.dat = w.04, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

modelno = "02"
keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

modelno = "03"
keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))
}
# A: All Waze ----

# 18 Base: All Waze features from event type (but not the counts of all Waze events together)
# 19 Add FARS only
# 20 Add Weather only
# 21 Add road class, AADT only
# 22 Add jobs only
# 23 Add all together

omits = c(alwaysomit, 
          "wx",
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

modelno = "18"

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              #thin.dat = 0.1,
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# 19, add FARS
modelno = "19"

for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "FARS_MD_2012_2016_sum_annual")
}

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# 20 Add Weather only
modelno = "20"

omits = c(alwaysomit, 
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# 21 Add road class, AADT only
modelno = "21"

for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_AADT_total_sum")
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum")
  }

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

omits = c(alwaysomit, 
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          "wx",
          grep("WAC", names(w.04), value = T), # jobs workplace
          grep("RAC", names(w.04), value = T), # jobs residence
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# 22 Add jobs only
modelno = "22"

for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum")
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum")
}

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

omits = c(alwaysomit, 
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT"), # AADT
          "wx",
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# 23 Add all together
modelno = "23"

omits = c(alwaysomit, 
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# B: TypeCounts

# 24 Base: nWazeAccident, nWazeJam, nWazeWeatherOrHazard, nWazeRoadClosed
# 25 Add other Waze only (confidence, reliability, magvar, neighbors)
# 26 Add FARS only
# 27 Add Weather only
# 28 Add road class, AADT only
# 29 Add jobs only
# 30 Add all (but not Waze event subtypes or sub-subtypes)
# 31 Pick best and test removing EDT only rows
# 32 Pick best and test removing road closure only rows

omits = c(alwaysomit, alert_subtypes,
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          "wx",
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

modelno = "24"

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              #thin.dat = 0.1,
                              model.no = modelno, rf.inputs = rf.inputs) 

omits = c(alwaysomit, alert_subtypes,
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          "wx")

modelno = "25"

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              #thin.dat = 0.1, 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))


# C. SubtypeCounts

# 33 Base: nWazeAccidentMajor, nWazeAccidentMinor, nWazeJamModerate, nWazeJamHeavy, nWazeJamStandStill, nHazardOnRoad, nHazardOnShoulder, nHazardWeather
# 34 Add other Waze only (confidence, reliability, magvar, neighbors)
# 35 Add FARS only
# 36 Add Weather only
# 37 Add road class, AADT only
# 38 Add jobs only
# 39 Add all (but not Waze event sub-subtypes)
# 40 Pick best and test removing EDT only rows
# 41 Pick best and test removing road closure only rows

omits = c(alwaysomit, alert_types, 
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          "wx",
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

modelno = "33"

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                             # thin.dat = 0.1, 
                              model.no = modelno, rf.inputs = rf.inputs) 

omits = c(alwaysomit, alert_types, 
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          "wx")

modelno = "34"

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              #thin.dat = 0.1, 
                              model.no = modelno, rf.inputs = rf.inputs) 



save("keyoutputs", file = paste0("Output_to_", modelno))

# D. EDT counts vs binary response: ----
 
# 42 Run best combination of each base model (A, B, and C) on counts vs binary
# 43 Pick best and test removing EDT only rows
# 44 Pick best and test removing road closure only rows

# E. Buffer vs grid cell counts for response variable ----

# 45 Run best combination of each base model (A, B, and C) on buffer counts vs grid cell counts
# 46 Pick best and test removing EDT only rows
# 47 Pick best and test removing road closure only rows

save("keyoutputs", file = paste0("Output_to_", modelno))
