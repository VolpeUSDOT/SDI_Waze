# Starting random forest model of crash estimation for TN


# Setup ---- 
rm(list=ls()) # Start fresh
library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

codeloc <- "~/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

grids = c("TN_01dd_fishnet",
          "TN_1sqmile_hexagons")

codeloc <- "~/SDI_Waze" 

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory 
localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

setwd(localdir)

# <><><><><>
g = grids[2] # start with square grids
state = "TN"
# <><><><><>

# Manually setting months to run here; could also scan S3 for months available for this state
do.months = c(paste("2017", c("04","05","06","07","08","09", "10", "11", "12"), sep="-"),
              paste("2018", c("01","02","03"), sep="-"))

# do.months = paste("2018", c("01","02","03"), sep="-")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

outputdir <- file.path(localdir, "Random_Forest_Output")
# Make outputdir if not alreday there
system(paste('mkdir -p', outputdir))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# TODO: update for TN additional variables

# # check if already completed transfer of necessary supplemental data on this instance
# if(length(dir(localdir)[grep("aadt_by_grid", dir(file.path(localdir, 'AADT')))]) == 0){
# 
#   source(file.path(codeloc, "utility/Workstation_setup.R"))
#   # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
#   system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
# }

# View the files available in S3 for this state: system(paste0('aws s3 ls ', teambucket, '/', state, '/'))

# rename data files by month. For each month, prep time and response variables
# See prep.hex() in wazefunctions.R for details.
for(mo in do.months){
  prep.hex(paste0("WazeTimeEdtHexAll_", mo, "_", g,".RData"), state = state, month = mo)
}

# Plot to check grid IDs. Requires shapefiles to be present in ~/workingdata/Hex, download from S3 if not there.
# This was useful when testing different grid sizes, to make sure everything was matching correctly.
CHECKPLOT = F
if(CHECKPLOT){
  usemonth = do.months[1]
  usedata <- ls()[grep(sub("-", "_", usemonth), ls())]
  
  grid_shp <- rgdal::readOGR(file.path(localdir, "Shapefiles"), layer = g)
  
  w.g <- match(get(usedata)$GRID_ID, grid_shp$GRID_ID)
  w.g <- w.g[!is.na(w.g)]
  gs <- grid_shp[w.g,]
  
  plot(gs, col = "red")
  rm(w.g, gs, grid_shp)
}

# TODO: figure out what supplemental variables to include from previous work
# Write function to apply special events and weather

# # Add FARS, AADT, HPMS, jobs
na.action = "fill0"

monthfiles = paste("w", do.months, sep=".")
monthfiles = sub("-", "_", monthfiles)

  
# Append supplemental data. 
# Just 2018 special event data for now; would like to get 2017 similar file for training.
source(file.path(codeloc, "TN", "utility", "Prep_SpecialEvents.R")) # gives spev.grid.time

for(w in monthfiles){ # w = "w.2017_04"
  
   append.hex2(hexname = w, data.to.add = "TN_SpecialEvent", state = state, na.action = na.action)
  
   # append.hex2(hexname = w, data.to.add = paste0("FARS_", state, "_2012_2016_sum_annual"), state = state, na.action = na.action)
   # 
   # # VMT
   # append.hex2(hexname = w, data.to.add = paste0(state, "_total_aadt_by_grid_fc_urban_VMTfactored"), state = state, na.action = na.action)
   # 
   # append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_wac_sum"), state = state, na.action = na.action)
   # append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_rac_sum"), state=state, na.action = na.action)
 }


# Bind all months together
w.allmonths.named <- monthfiles

w.allmonths <- vector()
for(i in w.allmonths.named){
  w.allmonths <- rbind(w.allmonths, get(i))
}

# Save compiled object 
save(w.allmonths, 
     file = file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', g, '.RData')))

# Output for Tableau
 # usevars = c("GRID_ID", names(w.allmonths)[c(8:25, 61:65, 80:85)])
 # 
 # write.csv(w.allmonths[usevars], 
 #      file = file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', g, '.csv')),
 #                                        row.names = F)

# format(object.size(w.allmonths), "Gb")

avail.cores = parallel::detectCores()

# if(avail.cores > 8) avail.cores = 12 # Limit usage below max if on r4.4xlarge instance. Comment this out to run largest models.

# Use this to set number of decision trees to use, and key RF parameters. mtry is especially important, should consider tuning this with caret package
# For now use same parameters for all models for comparision; tune parameters after models are selected
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("nTN", names(w.allmonths), value = T),
               grep("MatchTN", names(w.allmonths), value = T),
               grep("TN_UA", names(w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchTN_buffer_Acc"


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
                                omits, response.var = "MatchTN_buffer_Acc", 
                               # thin.dat = 0.01,
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
  } else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
}


save("keyoutputs", file = paste0("TN_Output_to_", modelno))

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)


# <><><><><><><><><><><><><><><><><><> 2018-12-17 Dan stopped here! 

READY = F # Use this to skip models which aren't ready for multi-state implementation yet 2018-08-15

if(READY){

  # 19, add FARS
modelno = "19"

omits = c(alwaysomit,
          "wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

 save("keyoutputs", file = paste0("Output_to_", modelno))
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 20 Add Weather only
modelno = "20"

omits = c(alwaysomit,
          #"wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){

keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits = omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 21 Add road class, AADT only
modelno = "21"

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
#          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
#          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits = omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 22 Add jobs only
modelno = "22"

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
#          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
#          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits = c(omits), response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else { 
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
  }
# 23 Add all together
modelno = "23"

omits = c(alwaysomit,
#          "wx",
#          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
#          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
#          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
#          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
#          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits = c(omits), response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}


} # end READY

# B: TypeCounts ----

# 24 Base: nWazeAccident, nWazeJam, nWazeWeatherOrHazard, nWazeRoadClosed
modelno = "24"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 

}
# 25 Add other Waze only (confidence, reliability, magvar, neighbors)
modelno = "25"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T) # Jobs residential
#          grep("MagVar", names(w.allmonths), value = T), # direction of travel
#          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
#          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
#          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

if(READY){
  
# 26 Add FARS only
modelno = "26"

omits = c(alwaysomit, alert_subtypes,
          "wx",
#          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 27 Add Weather only
modelno = "27"

omits = c(alwaysomit, alert_subtypes,
          #"wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 28 Add road class, AADT only
modelno = "28"

omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          #c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 29 Add jobs only
modelno = "29"
omits = c(alwaysomit, alert_subtypes,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          #grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
  }

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
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, omits, response.var = "MatchTN_buffer_Acc",  
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths,
                                        omits, response.var = "MatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


# 31 Pick best and test removing EDT only rows
# Use model 30 as base 

modelno = "31"

EDTonlyrows <- apply(w.allmonths[c(alert_types, alert_subtypes)], 1, FUN = function(x) all(x == 0))
summary(EDTonlyrows)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!EDTonlyrows,], omits, response.var = "MatchTN_buffer_Acc",  
                                 model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
    redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!EDTonlyrows,], 
                                        omits, response.var = "MatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

# 32 Pick best and test removing road closure only rows

modelno = "32"

Road.closure.onlyrows <- apply(w.allmonths[c("nWazeRoadClosed","nWazeAccident","nWazeJam","nWazeWeatherOrHazard", alert_subtypes)], 1, FUN = function(x) all(x[2:21] == 0) & x[1] > 0)
summary(Road.closure.onlyrows)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], omits, response.var = "MatchTN_buffer_Acc",  
                                 model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
    redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], 
                                        omits, response.var = "MatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

} # end READY

# C. SubtypeCounts ----

# 33 Base: nWazeAccidentMajor, nWazeAccidentMinor, nWazeJamModerate, nWazeJamHeavy, nWazeJamStandStill, nHazardOnRoad, nHazardOnShoulder, nHazardWeather

modelno = "33"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))

} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 34 Add other Waze only (confidence, reliability, magvar, neighbors)
modelno = "34"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T) # Jobs residential
          # grep("MagVar", names(w.allmonths), value = T), # direction of travel
          # grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          # grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          # grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
          )

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 

} else { 
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}


if(READY){

# 35 Add FARS only
modelno = "35"

omits = c(alwaysomit, alert_types,
          "wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 36 Add Weather only
modelno = "36"

omits = c(alwaysomit, alert_types,
          # "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 37 Add road class, AADT only
modelno = "37"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          # grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          # c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 38 Add jobs only
modelno = "38"

omits = c(alwaysomit, alert_types,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          # grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          # grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}
# 39 Add all (but not Waze event sub-subtypes)
modelno = "39"

omits = c(alwaysomit, alert_types
          # "wx",
          # c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          # grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          # c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          # grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          # grep("RAC", names(w.allmonths), value = T), # Jobs residential
          # grep("MagVar", names(w.allmonths), value = T), # direction of travel
          # grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          # grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          # grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
  
} else {
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
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
          #          grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          #          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #          grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          #          grep("RAC", names(w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "43a"

# Based on model 23, without EDT-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!EDTonlyrows,], 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!EDTonlyrows,], 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "44a"

# Based on model 23, without road closure-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "42b"

# Based on model 30
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
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "43b"

# Based on model 30, without EDT-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!EDTonlyrows,], 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!EDTonlyrows,], 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


modelno = "44b"

# Based on model 30, without road closure-only rows

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], 
                                omits, response.var = "nMatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  keyoutputs[[modelno]] = reassess.rf(train.dat = w.allmonths[!Road.closure.onlyrows,], 
                                        omits, response.var = "nMatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


} # end READY

# E. Buffer vs grid cell counts for response variable ----

# 45 Run best combination of each base model (A, B, and C) on buffer counts vs grid cell counts
# 46 Pick best and test removing EDT only rows
# 47 Pick best and test removing road closure only rows

# modelno = "45a"
# 
# modelno = "45b"


if(REASSESS) {
  save("keyoutputs", file = paste0(state, "_Reassess_Output_to_", modelno)) 
} else {
  save("keyoutputs", file = paste0(state, "_Output_to_", modelno))
  
}


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")

# zip and export outputs

outputdir = file.path(localdir, 'Random_Forest_Output')

# Using system zip:
# system(paste('zip ~/workingdata/zipfilename.zip ~/path/to/your/file'))

zipname = paste0('TN_RandomForest_Outputs_', g, "_", Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(localdir, 'TN_Output_to_34'),
             file.path(outputdir, 'TN_Model_18_RandomForest_Output.RData'),
             file.path(outputdir, 'TN_Model_24_RandomForest_Output.RData'),
             file.path(outputdir, 'TN_Model_25_RandomForest_Output.RData'),
             file.path(outputdir, 'TN_Model_33_RandomForest_Output.RData'),
             file.path(outputdir, 'TN_Model_34_RandomForest_Output.RData')
             ))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

