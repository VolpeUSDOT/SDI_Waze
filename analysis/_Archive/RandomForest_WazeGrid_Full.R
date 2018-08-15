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


# Filter for days with good EDT data, meaning day < 259 in our initial sample. This cuts off the last 12 days of September 2017.
w.04_09 = w.04_09 %>% filter(day < 259) 

# A: All Waze ----

# 18 Base: All Waze features from event type (but not the counts of all Waze events together)
# 19 Add FARS only
# 20 Add Weather only
# 21 Add road class, AADT only
# 22 Add jobs only
# 23 Add all together
starttime = Sys.time()

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

modelno = "18"

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                                omits, response.var = "MatchEDT_buffer_Acc", 
                               # thin.dat = 0.01,
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
  } else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
}

# 19, add FARS
modelno = "19"

omits = c(alwaysomit,
          "wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

 save("keyoutputs", file = paste0("Output_to_", modelno))
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 20 Add Weather only
modelno = "20"

omits = c(alwaysomit,
          #"wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){

keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 21 Add road class, AADT only
modelno = "21"

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
#          grep("F_SYSTEM", names(w.04), value = T), # road class
#          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 22 Add jobs only
modelno = "22"

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
#          grep("WAC", names(w.04), value = T), # Jobs workplace
#          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else { 
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
  }
# 23 Add all together
modelno = "23"

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

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# B: TypeCounts ----

# 24 Base: nWazeAccident, nWazeJam, nWazeWeatherOrHazard, nWazeRoadClosed
modelno = "24"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 

}
# 25 Add other Waze only (confidence, reliability, magvar, neighbors)
modelno = "25"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T) # Jobs residential
#          grep("MagVar", names(w.04), value = T), # direction of travel
#          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
#          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
#          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 26 Add FARS only
modelno = "26"

omits = c(alwaysomit, alert_subtypes,
          "wx",
#          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 27 Add Weather only
modelno = "27"

omits = c(alwaysomit, alert_subtypes,
          #"wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 28 Add road class, AADT only
modelno = "28"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.04), value = T), # road class
          #c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 29 Add jobs only
modelno = "29"
omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #grep("WAC", names(w.04), value = T), # Jobs workplace
          #grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
  }

# 30 Add all (but not Waze event subtypes or sub-subtypes)
modelno = "30"
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

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09,
                                        omits, response.var = "MatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


# 31 Pick best and test removing EDT only rows
# Use model 30 as base 

modelno = "31"

EDTonlyrows <- apply(w.04_09[c(alert_types, alert_subtypes)], 1, FUN = function(x) all(x == 0))
summary(EDTonlyrows)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!EDTonlyrows,], omits, response.var = "MatchEDT_buffer_Acc",  
                                 model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
    redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09[!EDTonlyrows,], 
                                        omits, response.var = "MatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

# 32 Pick best and test removing road closure only rows

modelno = "32"

Road.closure.onlyrows <- apply(w.04_09[c("nWazeRoadClosed","nWazeAccident","nWazeJam","nWazeWeatherOrHazard", alert_subtypes)], 1, FUN = function(x) all(x[2:21] == 0) & x[1] > 0)
summary(Road.closure.onlyrows)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!Road.closure.onlyrows,], omits, response.var = "MatchEDT_buffer_Acc",  
                                 model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
    redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09[!Road.closure.onlyrows,], 
                                        omits, response.var = "MatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

# C. SubtypeCounts ----

# 33 Base: nWazeAccidentMajor, nWazeAccidentMinor, nWazeJamModerate, nWazeJamHeavy, nWazeJamStandStill, nHazardOnRoad, nHazardOnShoulder, nHazardWeather

modelno = "33"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))

} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 34 Add other Waze only (confidence, reliability, magvar, neighbors)
modelno = "34"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T) # Jobs residential
          # grep("MagVar", names(w.04), value = T), # direction of travel
          # grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          # grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          # grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

} else { 
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 35 Add FARS only
modelno = "35"

omits = c(alwaysomit, alert_types,
          "wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 36 Add Weather only
modelno = "36"

omits = c(alwaysomit, alert_types,
          # "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 37 Add road class, AADT only
modelno = "37"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          # grep("F_SYSTEM", names(w.04), value = T), # road class
          # c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 38 Add jobs only
modelno = "38"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          # grep("WAC", names(w.04), value = T), # Jobs workplace
          # grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 39 Add all (but not Waze event sub-subtypes)
modelno = "39"

omits = c(alwaysomit, alert_types
          # "wx",
          # c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          # grep("F_SYSTEM", names(w.04), value = T), # road class
          # c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          # grep("WAC", names(w.04), value = T), # Jobs workplace
          # grep("RAC", names(w.04), value = T), # Jobs residential
          # grep("MagVar", names(w.04), value = T), # direction of travel
          # grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          # grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          # grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits, response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
  
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                      omits, response.var = "MatchEDT_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 40 Pick best and test removing EDT only rows - skipped, these are clearly worse models

# 41 Pick best and test removing road closure only rows


# D. EDT counts vs binary response: ----
 
# 42 Run best combination of each base model (A, B, and C) on counts vs binary
# 43 Pick best and test removing EDT only rows
# 44 Pick best and test removing road closure only rows

modelno = "42a"

# Based on model 23
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

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "43a"

# Based on model 23, without EDT-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!EDTonlyrows,], 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09[!EDTonlyrows,], 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "44a"

# Based on model 23, without road closure-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!Road.closure.onlyrows,], 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09[!Road.closure.onlyrows,], 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "42b"

# Based on model 30
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


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "43b"

# Based on model 30, without EDT-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!EDTonlyrows,], 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09[!EDTonlyrows,], 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "44b"

# Based on model 30, without road closure-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09[!Road.closure.onlyrows,], 
                                omits, response.var = "nMatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.04_09[!Road.closure.onlyrows,], 
                                        omits, response.var = "nMatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


# E. Buffer vs grid cell counts for response variable ----

# 45 Run best combination of each base model (A, B, and C) on buffer counts vs grid cell counts
# 46 Pick best and test removing EDT only rows
# 47 Pick best and test removing road closure only rows

# modelno = "45a"
# 
# modelno = "45b"

if(REASSESS) save("keyoutputs", file = paste0("Reassess_Output_to_", modelno))


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")


