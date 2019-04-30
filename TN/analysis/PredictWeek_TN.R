# Using best random forest models of crash estimation for TN, predict the number of crashes for the next week by grid ID and time.
# Inputs:
# - Time variables for today to next 10 days, for each Grid ID
# - Special events values prepped by Grid ID 
# - Weather forecast for by Grid ID
# - Waze input variables for model 05 (all TN crashes, Base Waze inputs): 
# - Join with all other predictors which are not time-varying, namely urban area, TotalHistCrash, TotalFatalCrash.

# Goal should be to source one script of each type fo input to prep, then join them all into a `next_week` data table. Then run predict(rf_05, next_week) to generate predictions for each grid cell, each hour of next week.

# Setup ---- 
rm(list=ls()) # Start fresh
codeloc <- "~/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

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

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

setwd(localdir)

# <><><><><>
g = grids[1] # start with square grids, now running hex also. Change between 1 and 2.
state = "TN"
# <><><><><>

# Load a fitted model from local machine -- run RandomForest_WazeGrids_TN.R to generate models

outputdir <- file.path(localdir, "Random_Forest_Output")

# Load data used for fitting - prepared also in RandomForest_wazeGrids_TN.R

Waze_Prepared_Data = dir(localdir)[grep(paste0('^', state, '_\\d{4}-\\d{2}_to_\\d{4}-\\d{2}_', g, '.RData'), dir(localdir))]

load(file.path(localdir, Waze_Prepared_Data))

# Get special events for next week ----

# Start with last week of 2018; need to get 2019. This is created by Prep_SpecialEvents.R
load(file.path(localdir, 'SpecialEvents', paste0('Prepared_TN_SpecialEvent_', g, '.RData')))

# Get weather for next week ----

source(file.path(codeloc, 'TN', 'datacleaning', 'Get_weather_forecasts.R'))

# Generate Waze events for next week ----
