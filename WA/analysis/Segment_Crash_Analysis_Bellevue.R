# Analysis of crashes on Bellevue road network segmetns.
# Analysis approaches:
# - Simple linear regression for count of crashes
# - Simple logistic regression for presence of crashes
# - Zero-inflated negative binomial
# - Hierarchical by road class..
# - Also do random forests / xgboost? Perhaps, but likely not as useful in this context since we want to interpret coefficients of individual factors
# In each of these, test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh
library(tidyverse)

codeloc <- ifelse(grep('Flynn', normalizePath('~/')),
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze")

source(file.path(codeloc, 'utility/get_packages.R'))

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
output.loc <- file.path(data.loc, "Segments")

setwd(data.loc)

# Check if prepared data are available; if not, run Segment Aggregation.

Waze_Prepared_Data = dir(output.loc)[grep("^Bellevue_Waze_Segments_", dir(output.loc))]

if(length(grep(Waze_Prepared_Data, dir(output.loc))) == 0){
  stop(paste("No Bellevue segment data available in", output.loc, "\n Run Segment_Aggregation_Bell.R or check network connection"))
}  else {
  load(file.path(output.loc, Waze_Prepared_Data))
}


# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start from prepared data
# Omit as predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all), value = T), "year", "day", "segtime", "weekday", 
               grep("Crash", names(w.all), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "uniqueCrashreports"

# TODO: Design sets of models to test ----
# Start simple
# 01 Base: All Waze features from event type (but not the counts of all Waze events together)
# 02 Add FARS only
# 03 Add Weather only

starttime = Sys.time()

# Make a list of variables to omit from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

omits = c(alwaysomit,
#         alert_types,
          alert_subtypes,
          "nFARS",                                  # FARS variables, 
          c('PRCP', 'TMIN', 'TMAX', 'SNOW'),        # wx variables
          grep("WazeRT", names(w.all), value = T),  # road types from Waze
          grep("MagVar", names(w.all), value = T)   # direction of travel
          )

modelno = "01"

# Simple 

( predvars = names(w.all)[!names(w.all) %in% omits] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       lm(use.formula, data = w.all)
       )

# Summarize


summary(get(paste0('m', modelno)))


summary.aov(get(paste0('m', modelno)))

# TODO: extract model diagnostics, save in a list..

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)

