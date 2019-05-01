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
library(pscl) # zero-inflated Poisson
library(MASS) # NB model
library(randomForest) # random forest


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
table(w.all.4hr.wd$ArterialCl)

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, "other_var" = other_var, "time_var" = time_var, "weather_var" = weather_var, "alert_types" = alert_types, "alert_subtypes" = alert_subtypes, "waze_rd_type" = waze_rd_type, "waze_dir_travel" = waze_dir_travel)

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports", # number of crashes at each segment at every time segment (every 4 hours at each weekday)
                  "biCrash",            # presence and absence of crash at each segment at every hour of day
                  "nCrashes"            # total crashes at each segment of entire year 2018
                  )

# Any last-minute data organization: order the levels of weekday
# w.all.4hr.wd$wkday = factor(w.all.4hr.wd$wkday, levels(w.all.4hr.wd$wkday)[c(4,2,6,7,5,1,3)])
w.all.4hr.wd$wkday.s = w.all.4hr.wd$wkday
levels(w.all.4hr.wd$wkday.s) <- c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")

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

# check missing values and all zero columns ----
# if any other columns are all zeros
all_var <- response.var.list
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

any(sapply(w.all.4hr.wd[, all_var], function(x) all(x == 0))) # Returns FALSE if no columns have all zeros
# all variables of w.all.4hr.wd are clean now. None of them are all-zero column. 

# Check time variables & Time Series visuals ----
stopifnot(length(unique(w.all.4hr.wd$wkday)) == 7)
stopifnot(length(unique(w.all.4hr.wd$grp_hr)) == 6)

## ggplot of time series
f <- paste0(visual.loc, '/Bellevue_crashes_time_series_4hr_wd.png')

# use minimal format
theme_set(theme_minimal())

png(f, width = 6, height = 10, units = 'in', res = 300)

# by both
p1 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = grp_hr, y = uniqueCrashreports)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("wkday.s") +
  ylab("Number of Crashes")

p2 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = wkday.s, y = uniqueCrashreports)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("grp_hr") +
  ylab("Number of Crashes")

# by wkday
p3 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s), aes(x = wkday.s, y = uniqueCrashreports)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Number of Crashes") +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90))

# by 4-hour window
p4 <- ggplot(data = ts_group_by(w.all.4hr.wd, grp_hr), aes(x = grp_hr, y = uniqueCrashreports)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Number of Crashes") 

multiplot(p1, p2, p3, p4)

dev.off()


# Mean and variance ----
hist(w.all.4hr.wd$uniqueCrashreports)
mean(w.all.4hr.wd$uniqueCrashreports) # 0.106
var(w.all.4hr.wd$uniqueCrashreports) # 0.1179727
# The data is not overdispersed. Maybe a Poisson model is also appropriate.
ggplot(w.all.4hr.wd, aes(uniqueCrashreports)) + geom_histogram() + scale_x_log10()

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start Modelings ----
# Start simple
# 10: seg_var + TimeOfDay + DayofWeek + Month
# 11: 10 + nFARS
# 12: 10 + nFARS + nBikes
# 13: {Best of 10 - 12} = 12 + weather_var
# 14: {Best of 10 - 13} = 12 + alert_types
# 15: 14 + waze_dir_travel + waze_rd_type

starttime = Sys.time()

# Make a list of variables to include from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

includes = c(
  waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  # weather_var,     # Weather variables
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS",         # FARS variables
  seg_var, "wkday", "grp_hr"
)

modelno = "15.poi"

response.var <- response.var.list[1] # use crash counts

Art.Only <- F # False to use all road class, True to Arterial only roads
if(Art.Only) {data = w.all.4hr.wd %>% filter(ArterialCl != "Local")} else {data = w.all.4hr.wd} 

# Simple 

( predvars = names(w.all.4hr.wd)[names(w.all.4hr.wd) %in% includes] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       glm(use.formula, data = data, family=poisson) # regular Poisson Model
       # zeroinfl(use.formula, data = data) # zero-inflated Possion
       # glm.nb(use.formula, data = data) # regular NB
       # zeroinfl(use.formula, data = data, dist = "negbin", EM = F) # zero-inflated NB, EM algorithm looks for optimal starting values, which is slower.
)

# Summarize
summary(get(paste0('m', modelno)))

fitted <- fitted(get(paste0('m', modelno)))
pres <- residuals(get(paste0('m', modelno)), type="pearson")
assign(paste0('diag.plot.', modelno), plot((fitted)^(1/2), abs(pres)))

AIC(m10.0poi, m10.poi, m10.nb, m10.0nb) # we ran a base model 10, and here is the comparison of AIC.
# model    df      AIC
# m10.0poi 36 8832.233
# m10.poi  18 8882.401
# m10.nb   19 8850.987
# m10.0nb  37 8831.231

# plot(get(paste0('m', modelno)), which=3)

AIC(m10.poi, m11.poi, m12.poi, m13.poi, m14.poi, m15.poi) # model 12 seems to be slightly better, adding weather does not improve, so will exclude weather out of the model. Model 15 is the best, adding Waze data helps a lot.
# model   df      AIC
# m10.poi 18 8882.401
# m11.poi 19 8884.033
# m12.poi 20 8879.510
# m13.poi 24 8879.688
# m14.poi 24 8615.698
# m15.poi 35 7632.854

# Random Forest and XGBoost ----
# https://xgboost.readthedocs.io/en/latest/R-package/xgboostPresentation.html

randomForest(use.formula, data = data) # RF model

