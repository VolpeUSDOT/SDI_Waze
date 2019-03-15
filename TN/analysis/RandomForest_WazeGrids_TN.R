# Random forest models of crash estimation for TN
# 2019-02: Now focusing just on TN_crash as response, stil with both grid IDs. 

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

user <- if(length(grep("@securedatacommons.com", getwd())) > 0) {
  paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
} else {
  paste0( "/home/", system("whoami", intern = TRUE))
} # find the user directory to use

localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "TN", "utility/wazefunctions_TN.R")) 

setwd(localdir)

# <><><><><>
g = grids[1] # start with square grids, now running hex also. Change between 1 and 2.
state = "TN"
# <><><><><>

# Manually setting months to run here; could also scan S3 for months available for this state
do.months = c(paste("2017", c("04","05","06","07","08","09", "10", "11", "12"), sep="-"),
              paste("2018", c("01","02","03"), sep="-"))#,"04","05","06","07","08","09"), sep="-"))

# do.months = paste("2018", c("01","02","03"), sep="-")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

outputdir <- file.path(localdir, "Random_Forest_Output")
# Make outputdir if not alreday there
system(paste('mkdir -p', outputdir))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# View the files available in S3 for this state: system(paste0('aws s3 ls ', teambucket, '/', state, '/'))

Waze_Prepared_Data = paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', g)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start data prep. Run this if the data for this time period and grid size are not ready yet, otherwise read from prepared data.

if(length(grep(Waze_Prepared_Data, dir(localdir))) == 0){
  
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

na.action = "fill0" # This is an argument in append.hex, below. Other options are 'omit' or 'keep'.
monthfiles = paste("w", do.months, sep=".")
monthfiles = sub("-", "_", monthfiles)

# Append supplemental data ----

# Both 2017 and 2018 special event data now
source(file.path(codeloc, "TN", "utility", "Prep_SpecialEvents.R")) # gives spev.grid.time and spev.grid.time.holiday. Prep of 1sqmile ~ 4 hours, 01dd ~ 5 min.

source(file.path(codeloc, "TN", "utility", "Prep_HistoricalCrash.R")) # gives crash

source(file.path(codeloc, "TN", "utility", "Prep_HistoricalWeather.R")) # gives wx.grd.day. Weather variables, by grid ID, by day. Takes ~ 2 hrs for 0.1 dd on 16 core instance.
# Run for 1sqmile hex, after optimizing now approx 20 hours.

# Add prepared special events and historical crash data, with grid ID
for(w in monthfiles){ # w = "w.2017_04"
   cat(w, ". ")
   append.hex(hexname = w, data.to.add = "TN_SpecialEvent", state = state, na.action = na.action)

   append.hex(hexname = w, data.to.add = "crash", state = state, na.action = na.action)
   
   append.hex(hexname = w, data.to.add = "wx.grd.day", state = state, na.action = na.action)
 
}

# Bind all months together
w.allmonths.named <- monthfiles

w.allmonths <- vector()
for(i in w.allmonths.named){
  w.allmonths <- rbind(w.allmonths, get(i))
}

# Save compiled object 
save(w.allmonths, 
     file = file.path(localdir, paste0(Waze_Prepared_Data, ".RData")))

# Output for Tableau
 usevars = c("GRID_ID", names(w.allmonths)[c(8:44, 63:69, 83:ncol(w.allmonths))])

 write.csv(w.allmonths, #[usevars],
      file = file.path(localdir, paste0(Waze_Prepared_Data, ".csv")),
                                        row.names = F)

# format(object.size(w.allmonths), "Gb")
# To save for export, after running both grid sizes for these months:
# source(file.path(codeloc, "TN", "analysis/scratch/bundle_RF_inputs.R"))
 
} # End data prep 
# <><><><><><><><><><><><><><><><><><><><><><><><>


# Start from prepared data
 
load(file.path(localdir, paste0(Waze_Prepared_Data, ".RData")))
                  
avail.cores = parallel::detectCores()

# if(avail.cores > 8) avail.cores = 12 # Limit usage below max if on r4.4xlarge instance. Comment this out to run largest models.

# Use this to set number of decision trees to use, and key RF parameters. mtry is especially important, should consider tuning this with caret package
# For now use same parameters for all models for comparision; tune parameters after models are selected
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", 'wDate', 'wHour', 'tDate', 'tHour',
             # "uniqueWazeEvents", 
               "nWazeRowsInMatch", 
               "uniqueTNreports", "TN_crash", "date",
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("nTN", names(w.allmonths), value = T),
               grep("MatchTN", names(w.allmonths), value = T),
               grep("TN_UA", names(w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchTN_buffer_Acc" # now could use nTN_total, for all TN crashes in this grid cell, this hour. Or TN_crash, binary indicator of if any TN crash occurred in this grid cell/hours 

starttime = Sys.time()

# A: Hourly, match buffer ----

# 01 Base: Omit all Waze input, just special events and historical crashes


omits = c(alwaysomit,
          "uniqueWazeEvents",
          grep("nWaze", names(w.allmonths), value = T), # All Waze events
          alert_subtypes,
          grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

# Check to see what we are passing as predictors
names(w.allmonths)[is.na(match(names(w.allmonths), omits))]

modelno = paste("01", g, sep = "_")

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "MatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs,
                                cutoff = c(0.95, 0.05)
                                #, thin.dat = 0.2
                                ) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
  } else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
}

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)


# 02, add base Waze features
modelno = paste("02", g, sep = "_")

omits = c(alwaysomit,
          "uniqueWazeEvents",
           grep("nWazeRT", names(w.allmonths), value = T), # All Waze road type
          # grep("nWaze", names(w.allmonths), value = T), # All Waze events
          alert_subtypes,
          grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                              omits, response.var = "MatchTN_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs,
                              cutoff = c(0.95, 0.05)) 

 save("keyoutputs", file = paste0("Output_to_", modelno))
} else {

redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                      omits, response.var = "MatchTN_buffer_Acc", 
                                      model.no = modelno, rf.inputs = rf.inputs) 
}

# 03, add all Waze features
modelno = paste("03", g, sep = "_")

omits = c(alwaysomit
          #grep("nWaze", names(w.allmonths), value = T), # All Waze events
          #alert_subtypes,
          #grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          #grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          #grep("MagVar", names(w.allmonths), value = T), # direction of travel
          #grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "MatchTN_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs,
                                cutoff = c(0.95, 0.05)) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "MatchTN_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}


# B: Hourly, response is all TN crashes, binary ----

# 04 - No Waze info
# 05 - Add base Waze variables
# 06 - Add all Waze variables

omits = c(alwaysomit,
          "uniqueWazeEvents",
          grep("nWazeRT", names(w.allmonths), value = T), # All Waze road type
          grep("nWaze", names(w.allmonths), value = T), # All Waze events
          alert_subtypes,
          grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

# Check to see what we are passing as predictors
names(w.allmonths)[is.na(match(names(w.allmonths), omits))]

modelno = paste("04", g, sep = "_")

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "TN_crash", 
                                model.no = modelno, rf.inputs = rf.inputs,
                                cutoff = c(0.9, 0.1)) 
  
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "nTN_total", 
                                        model.no = modelno, rf.inputs = rf.inputs)
}

# 05, add base Waze features
modelno = paste("05", g, sep = "_")

omits = c(alwaysomit,
          "uniqueWazeEvents",
          grep("nWazeRT", names(w.allmonths), value = T), # All Waze road type
          # grep("nWaze", names(w.allmonths), value = T), # All Waze events
          alert_subtypes,
          grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          grep("MagVar", names(w.allmonths), value = T), # direction of travel
          grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "TN_crash", 
                                model.no = modelno, rf.inputs = rf.inputs,
                                cutoff = c(0.9, 0.1))  
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "nTN_total", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}

# 06, add all Waze features
modelno = paste("06", g, sep = "_")

omits = c(alwaysomit
          #grep("nWaze", names(w.allmonths), value = T), # All Waze events
          #alert_subtypes,
          #grep("Waze_UA", names(w.allmonths), value = T), # Waze Urban Area
          #grep("nHazard", names(w.allmonths), value = T), # Waze hazards
          #grep("MagVar", names(w.allmonths), value = T), # direction of travel
          #grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.allmonths, 
                                omits, response.var = "TN_crash", 
                                model.no = modelno, rf.inputs = rf.inputs,
                                cutoff = c(0.9, 0.1))  
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.allmonths, 
                                        omits, response.var = "nTN_total", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}



timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")

# zip and export outputs

outputdir = file.path(localdir, 'Random_Forest_Output')

# Using system zip:
# system(paste('zip ~/workingdata/zipfilename.zip ~/path/to/your/file'))

zipname = paste0('TN_RandomForest_Outputs_', g, "_", Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(localdir, paste0('Output_to_06_', g)),
             file.path(outputdir, paste0('TN_Model_01_', g, '_RandomForest_Output.RData')),
             file.path(outputdir, paste0('TN_Model_02_', g, '_RandomForest_Output.RData')),
             file.path(outputdir, paste0('TN_Model_03_', g, '_RandomForest_Output.RData')),
             file.path(outputdir, paste0('TN_Model_04_', g, '_RandomForest_Output.RData')),
             file.path(outputdir, paste0('TN_Model_05_', g, '_RandomForest_Output.RData')),
             file.path(outputdir, paste0('TN_Model_06_', g, '_RandomForest_Output.RData'))
             
             ))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

