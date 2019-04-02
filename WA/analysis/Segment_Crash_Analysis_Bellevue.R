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
library(ggplot2)
require(GGally)

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

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
# Start from prepared data for 1 hour window
table(w.all$uniqueCrashreports) # only 3 segment have more than 1 crash, logistic regression is more appropriate for this data.
# 0     1     2 
# 59870  1364     3 

# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all), value = T), "year", "day", "segtime", "weekday", 
               grep("Crash", names(w.all), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all), value = T)[-2] # nMagVar30to60 is all zero, removing it from the variable list

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS")

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl"
            , "FunctionCl"
            ) # "ArterialCl" has no missing values. There are 6 rows with missing values in "FunctionCl", therefore if we use in the model, we will lose these rows.

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, "other_var" = other_var, "weather_var" = weather_var, "alert_types" = alert_types, "alert_subtypes" = alert_subtypes, "waze_rd_type" = waze_rd_type, "waze_dir_travel" = waze_dir_travel)

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports",
                  "biCrash",
                  "nCrashes"
                  )
# ncrash.1yr.excludeInt  # have not created yet, should be "nCrashes" - "Crash_End1" - "Crash_End2"
# ncrash.4hr = NA # if we use 4 hour window, have not created this variable yet


# Correlation ----
library(corrplot)
correlations <- cor(w.all[, c(response.var.list, "Shape_STLe")])
corrplot(correlations, method="circle", type = "upper", tl.col = "black"
         # , tl.srt = 45
         # , main = "Correlation Plots"
)

# ggpairs to look at both scatter, boxplot, and density plots, as well as correlation, tried to save as pdf, too slow to open.
# alternative: use boxplots for categorical variables

for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  png(file = paste0(data.loc,'/Model_visualizations/ggpairs_', names(indicator.var.list)[i],".png"),  
      width = celing(n/2)*2, # or use 12 for all of them 
      height = celing(n/2)*2,
      units = 'in', 
      res = 300)
  g <- ggpairs(w.all[, c(response.var.list, indicator.var.list[[i]])])
  print(g)
  
  dev.off()
  
}



# check missing values ----
# Checked all interested predictors listed above, they are good!
library(Amelia)
library(mlbench)
missmap(w.all[, alert_types], col = c("blue", "red"), legend = FALSE)


# TODO: Design sets of models to test ----
# Start simple
# 01 Base: Road network (seg_var)
# 02: Add FARS only ("nFARS")
# 03: Add Weather only (weather_var)
# 04: Add Waze features from event type (but not the counts of all Waze events together) alert_types, alert_subtypes
# 05: Add all other Waze features (waze_rd_type, waze_dir_travel)


starttime = Sys.time()

# Make a list of variables to omit from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

includes = c(
              # waze_rd_type,    # road types from Waze
              # waze_dir_travel, # direction of travel
              # alert_types,     # counts of waze events by alert types
              # alert_subtypes,  # counts of waze events by sub-alert types
              # weather_var,
              # "nFARS",         # FARS variables 
              # "nBikes",        # bike conflict counts
              seg_var
          )

modelno = "01"

response.var <- response.var.list[2]

# Simple 

( predvars = names(w.all)[names(w.all) %in% includes] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       # lm(use.formula, data = w.all) # Linear model
       glm(use.formula, data = w.all, family = "binomial") # logistic regression
       )

# Summarize

summary(get(paste0('m', modelno)))


summary.aov(get(paste0('m', modelno)))

# TODO, convert the n of crash to binary and do logistic regression
# once we aggregate to a 4 hour window or a day, the counts might change, then we will need a zero-inflated NB model.

# TODO: extract model diagnostics, save in a list..

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)

