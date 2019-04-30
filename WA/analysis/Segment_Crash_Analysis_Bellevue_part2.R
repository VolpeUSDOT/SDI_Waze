# Analysis of crashes on Bellevue road network segmetns.
# Analysis approaches:
# - Zero-inflated negative binomial
# - Random forest models
# - Hierarchical by road class..
# - Also do random forests / xgboost? Perhaps, but likely not as useful in this context since we want to interpret coefficients of individual factors
# In each of these, test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

source(file.path(codeloc, 'utility/get_packages.R'))
# load functions with group_by
source(file.path(codeloc, 'WA/utility/visual_fun.R'))

library(tidyverse)
library(ggplot2)
library(GGally)
library(dplyr)
library(corrplot)
library(Amelia)
library(mlbench)
library(xts)
library(lubridate)


## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
seg.loc <- file.path(data.loc, "Segments")
output.loc <- file.path(data.loc, "Model_output")
visual.loc <- file.path(data.loc, "Model_visualizations")

setwd(data.loc)

# Load both the 1hr and 4hr data
fn = "Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData"
load(file.path(seg.loc, fn))

# <><><><><><><><><><><><><><><><><><><><><><><><> Start from prepared data for 4 hour window
# 4-hr model variables organization:  ----
table(w.all.4hr.wd$uniqueCrashreports) # ~10% of the data has non-zero counts, 0.8% of the data has counts larger than 1
# 0     1     2     3     4 
# 11609  1146    81    16     3 

# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all.4hr.wd), value = T), "year", "wkday", "grp_name", "grp_hr", "nFARS_1217",
               grep("Crash", names(w.all.4hr.wd), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all), value = T)[-2] # nMagVar30to60 is all zero, removing it from the variable list

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS")

time_var = c("grp_hr", "wkday") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl"
            # , "FunctionCl"
            ) # "ArterialCl" is complete. There are 6 rows with missing values in "FunctionCl", therefore if we use in the model, we will lose these rows.

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, "other_var" = other_var, "time_var" = time_var, "weather_var" = weather_var, "alert_types" = alert_types, "alert_subtypes" = alert_subtypes, "waze_rd_type" = waze_rd_type, "waze_dir_travel" = waze_dir_travel)

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports", # number of crashes at each segment at every time segment (every 4 hours at each weekday)
                  "biCrash",            # presence and absence of crash at each segment at every hour of day
                  "nCrashes"            # total crashes at each segment of entire year 2018
                  )

# Any last-minute data organization: order the levels of weekday
w.all.4hr.wd$wkday = factor(w.all.4hr.wd$wkday, levels(w.all.4hr.wd$wkday)[c(4,2,6,7,5,1,3)])

# Correlation & ggpairs ----
correlations <- cor(w.all.4hr.wd[, c(response.var.list, "Shape_STLe")])
corrplot(correlations, method="circle", type = "upper",
         diag = F,
         tl.col = "black"
         # , tl.srt = 45
         # , main = "Correlation Plots"
) # segment length is only highly correlated with nCrashes, not 4hour crashes or biCrash

# ggpairs to look at scatter, boxplot, and density plots, as well as correlation, tried to save as pdf, too slow to open.
# confirm that the 4hr break has similar pattern
for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  f <- paste0(visual.loc, '/ggpairs_4hr_wd_', names(indicator.var.list)[i],".png")
  
  # if the file exists, then don't regenerate as it takes a few minutes for each plots
  if (!file.exists(f)) {
    png(file = f,  
        width = ceiling(n/2)*2, # or use 12 for all of them 
        height = ceiling(n/2)*2,
        units = 'in', 
        res = 300)
    g <- ggpairs(w.all.4hr.wd[, c(response.var.list, indicator.var.list[[i]])])
    
    print(g)
    
    dev.off()
  }
}

# 
