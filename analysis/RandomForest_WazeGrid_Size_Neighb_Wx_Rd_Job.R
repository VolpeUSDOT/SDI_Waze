# Random Forest work for Waze
# Set up for ATA
# Updated to use function

# Goals: produce random forest analysis for features of Waze accidents and other event types that predict matching with EDT reports. Test effects for two spatial scales (1 and 4 mile aggregations), with and without information from neighboring cells for Waze accidents and jams.

# For 1 mi grid cells, test effect of adding precipitation data (NEXRAD), jobs data from LODES, and road type data from HPMS.

# For each model, do the following tests:
# 1. April, 70/30 split
# 2. April + May, 70 / 30 split
# 3. April + May, predict June.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

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
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

do.months = c("04", "05", "06")

waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))
# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# Read in data ----
aws.signature::use_credentials()

# Grab any necessary data from the S3 bucket. This transfers all contents of MD_hexagons_shapefiles to the local instance. For this script, we will use shapefiles_funClass.zip (road classification data), shapefiles_rac.zip (Residence Area Characteristics), shapefiles_wac.zip (Workplace Area Characteristics). Unzip these into the local dir

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

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage to 10 cores if on r4.4xlarge instance

rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)
keyoutputs = list() # to store model diagnostics


# Analysis ----

# Models 01, 02, 03: 1 mile, no additional data, no neighbors.

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well. Here also omitting precipitation and neighboring grid cells
# Omit as predictors in this vector:
omits = c(alwaysomit,
          "wx",
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

# Model 01: April ----
modelno = "01"

keyoutputs[[modelno]] = do.rf(train.dat = w.04, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

# Model 02: April + May ----
modelno = "02"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 03: April + May, predict June ----
modelno = "03"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# cleanup
rm(w.04, w.05, w.06); gc()

# Model 04: April, 4 mi ----
# note file names is WazeTimeEdtHexAll, as weather data not prepped yet for 4 mi hexagons

HEXSIZE = "4"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

omits = c(alwaysomit,
          "wx",
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

modelno = "04"

keyoutputs[[modelno]] = do.rf(train.dat = w.04, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))


# Model 05: April + May, predict June, 4 mi ----
modelno = "05"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 


save("keyoutputs", file = paste0("Output_to_", modelno))

# cleanup
rm(w.04, w.05, w.06); gc()

# Model 06: April, 0.5 mi ----
# note file names is WazeTimeEdtHexAll, as weather data not prepped yet for 4 mi hexagons
HEXSIZE = "05"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,"_", HEXSIZE, "mi.RData")), month = mo)
}

modelno = "06"

keyoutputs[[modelno]] = do.rf(train.dat = w.04, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 07: April + May, predict June, 0.5 mi ----
modelno = "07"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                               omits, response.var = "MatchEDT_buffer_Acc", 
                               model.no = modelno, rf.inputs = rf.inputs) 

# cleanup
rm(w.04, w.05, w.06); gc()

# Model 08: April, 1 mi, neighbors ----
HEXSIZE = "1"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

modelno = "08"

omits = c(alwaysomit,
          "wx"
        #  grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
        #  grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

keyoutputs[[modelno]] = do.rf(train.dat = w.04, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 09: April + May, predict June, 1 mi, neighbors ----
modelno = "09"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(w.04, w.05, w.06); gc()

# Model 10: April, 4 mi, neighbors ----
# note file names is WazeTimeEdtHexAll, as weather data not prepped yet for 4 mi hexagons
HEXSIZE = "4"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

modelno = "10"

keyoutputs[[modelno]] = do.rf(train.dat = w.04,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 11: April + May, predict June, 4 mi, neighbors ----
modelno = "11"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(w.04, w.05, w.06); gc()

# Model 12: April, 1 mi, neighbors, wx ----
HEXSIZE = "1"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

omits = c(alwaysomit
          # "wx"
          #  grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          #  grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

modelno = "12"

keyoutputs[[modelno]] = do.rf(train.dat = w.04,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 13: April + May, predict June, 1 mi, neighbors, wx ----
modelno = "13"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))


# cleanup
rm(w.04, w.05, w.06);gc()

# Model 14: April, 1 mi, neighbors, wx, roads ----
# Models 14 and 15, adding road functional class
modelno = "14"

HEXSIZE = "1"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

for(w in c("w.04", "w.05", "w.06")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = "fill0")
}

omits = c(alwaysomit)

keyoutputs[[modelno]] = do.rf(train.dat = w.04,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

# Model 15: April + May, predict June, 1 mi, neighbors, wx, roads ----
modelno = "15"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm( w.04, w.05, w.06)

# Model 16: April, 1 mi, neighbors, wx, roads, jobs ----
modelno = "16"

HEXSIZE = "1"
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

# Add road functional class characteristics
for(w in c("w.04", "w.05", "w.06")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = "fill0")
}

# Add workplace area characteristics
for(w in c("w.04", "w.05", "w.06")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum", na.action = "fill0")
}

# Add residence area characteristics
for(w in c("w.04", "w.05", "w.06")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum", na.action = "fill0")
}

# Look at NA for road data (using na.action = "keep" in append.hex above)
r.na <- w.04[is.na(w.04$F_SYSTEM_V_1),]
head(r.na)
table(r.na$MatchEDT_buffer); table(r.na$nMatchEDT_buffer)
table(r.na$nWazeAccident)
# What percent of EDT crashes are we discarding? 1.3% of April data
100*length(r.na$MatchEDT_buffer_Acc==1)/length(w.04$MatchEDT_buffer_Acc==1)

omits = c(alwaysomit
          # grep("nWazeRT", names(w.04), value = T), # omit Waze RT
          # grep("nWazeJam_", names(w.04), value = T) # experiment: omit neighboring jams
)

keyoutputs[[modelno]] = do.rf(train.dat = w.04,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 


# Model 17: April + May, predict June, 1 mi, neighbors, wx, roads, jobs ----
modelno = "17"

keyoutputs[[modelno]] = do.rf(train.dat = rbind(w.04, w.05), test.dat = w.06,
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(w.04, w.05, w.06); gc()
