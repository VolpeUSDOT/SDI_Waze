# Bellevue - initial models to test
# Analysis approaches:
# - Simple linear regression for count of crashes
# - Simple logistic regression for presence of crashes
# - Zero-inflated negative binomial
# In each of these, test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

#source(file.path(codeloc, 'utility/get_packages.R'))
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

# <><><><><><><><><><><><><><><><><><><><><><><><> Start linear and logistic regression from prepared data for 1 hour window
# 1-hr model Variables organization:----
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

time_var = c("hour", "mo", "weekday", "day") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl"
            # , "FunctionCl"
            ) # "ArterialCl" has no missing values. There are 6 rows with missing values in "FunctionCl", therefore if we use in the model, we will lose these rows.

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, "other_var" = other_var, "weather_var" = weather_var, "alert_types" = alert_types, "alert_subtypes" = alert_subtypes, "waze_rd_type" = waze_rd_type, "waze_dir_travel" = waze_dir_travel)

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports", # number of crashes at each segment at every hour of day
                  "biCrash",            # presence and absence of crash at each segment at every hour of day
                  "nCrashes"            # total crashes at each segment of entire year 2018
                  
                  )
# ncrash.1yr.excludeInt  # have not created yet, should be "nCrashes" - "Crash_End1" - "Crash_End2"
# ncrash.4hr = NA # if we use 4 hour window, have not created this variable yet



# Correlation & ggpairs ----
# correlations <- cor(w.all[, c(response.var.list, "Shape_STLe")])
# corrplot(correlations, method="circle", type = "upper", 
#          diag = F,
#          tl.col = "black"
#          # , tl.srt = 45
#          # , main = "Correlation Plots"
# )

# ggpairs to look at scatter, boxplot, and density plots, as well as correlation, tried to save as pdf, too slow to open.
# alternative: use boxplots for categorical variables

for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  f <- paste0(visual.loc, '/ggpairs_', names(indicator.var.list)[i],".png")
  
  # if the file exists, then don't regenerate as it takes a few minutes for each plots
  if (!file.exists(f)) {
      png(file = f,  
          width = ceiling(n/2)*2, # or use 12 for all of them 
          height = ceiling(n/2)*2,
          units = 'in', 
          res = 300)
      g <- ggpairs(w.all[, c(response.var.list, indicator.var.list[[i]])])
      
      print(g)
      
      dev.off()
  }
}

# check missing values and all zero columns ----
# Checked all interested predictors listed above, they are good!
# missmap(w.all[, alert_types], col = c("blue", "red"), legend = FALSE)

for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  f <- paste0(visual.loc, '/missmap_', names(indicator.var.list)[i],".png")
  
  # if the file exists, then don't regenerate as it takes a few minutes for each plot
  if (!file.exists(f)) {
    png(file = f,  
        width = ceiling(n/2)*2, # or use 12 for all of them 
        height = ceiling(n/2)*2,
        units = 'in', 
        res = 300)
    missmap(w.all[, indicator.var.list[[i]]], col = c("blue", "red"), legend = FALSE)
    
    dev.off()
  }
}

# if any other columns are all zeros
all_var <- vector()
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

any(sapply(w.all[, all_var], function(x) all(x == 0))) # all variables are clean now. None of them are all-zero column. Returns FALSE if no columns have all zeros

# Check time variables & Time Series visuals ----
stopifnot(length(unique(w.all$day)) == 365) # 365
stopifnot(length(unique(w.all$mo)) == 12)   # 12
stopifnot(length(unique(w.all$hour)) == 24) # 24

# create a data frame with only one variable, including all hours in 2018
day.hour <- data.frame("time_hr" = seq(from = as.POSIXct("2018-01-01 0:00", tz = 'America/Los_Angeles'), 
                  to = as.POSIXct("2018-12-31 0:00", tz = 'America/Los_Angeles'),
                  by = "hour")
                  )
# day.hour <- as.character(day.hour)

# Create time_hr in w.all
w.all = w.all %>%
  mutate(time_hr = as.POSIXct(segtime, '%Y-%j %H', tz = 'America/Los_Angeles'))

# select a few variables as example
w.sub <- w.all[, c("time_hr", "RDSEG_ID", "uniqueCrashreports", "nCrashKSI", "uniqueWazeEvents", "nWazeAccident", "nWazeJam")]

# join day.hour with w.sub on time_hr, NA fields were converted to zeros.
w.sub <- day.hour %>% left_join(w.sub, by = c("time_hr")) %>% 
  mutate_if(is.numeric, coalesce, 0) %>% 
  mutate(
    year = format(time_hr, "%Y"),
    day = format(time_hr, "%j"),
    hour = format(time_hr, "%H"))

# check if the data is sorted by time, to prepare creation of a time series
stopifnot(!is.unsorted(w.sub$time_hr)) # the data now is sorted by time.

# Aggregate by time_hr, year, day, and hour
w.sub_seg <- w.sub %>% 
  group_by(time_hr, year, day, hour) %>% 
  summarize(
    uniqueCrashreports = sum(uniqueCrashreports),
    nCrashKSI = sum(nCrashKSI),
    uniqueWazeEvents = sum(uniqueWazeEvents),
    nWazeAccident = sum(nWazeAccident),
    nWazeJam =  sum(nWazeJam)
    )
w.sub_seg <- as.data.frame(w.sub_seg)
w.sub_seg <- w.sub_seg %>% mutate(time_hr = as.POSIXct(time_hr, '%Y-%j %H', tz = 'America/Los_Angeles'),
                                  date = as.Date(time_hr, format = '%Y-%j %H'),
                                  month = as.Date(cut(date, breaks = "month")),
                                  # week = as.Date(cut(date, breaks = "week")),
                                  weekday = as.factor(weekdays(date))
)

# order the levels of weekday
w.sub_seg$weekday = factor(w.sub_seg$weekday, levels(w.sub_seg$weekday)[c(4,2,6,7,5,1,3)])

# # Convert to timeseries
# df2 <- xts(x = w.sub_seg[!names(w.sub_seg) %in% 'time_hr'], order.by = w.sub_seg$time_hr)

# check if the data is sorted by time
stopifnot(!is.unsorted(w.sub_seg$time_hr)) # still in the order

## ggplot of time series
f <- paste0(visual.loc, '/Bellevue_WazeAccidents_time_series.png')

# use minimal format
theme_set(theme_minimal())

png(f, width = 12, height = 10, units = 'in', res = 300)

# by day hour
p1 <- ggplot(data = w.sub_seg, aes(x = time_hr, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("Waze Acc.")

# by day
p2 <- ggplot(data = ts_group_by(w.sub_seg, date), aes(x = date, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("Waze Acc.")

# by month
p3 <- ggplot(data = ts_group_by(w.sub_seg, month), aes(x = month, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("Waze Acc.") +
  scale_x_date(date_breaks = "1 month", date_labels = "%b")

# by weekday
p4 <- ggplot(data = ts_group_by(w.sub_seg, weekday), aes(x = weekday, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Waze Acc.")

# by hour
p5 <- ggplot(data = ts_group_by(w.sub_seg,  hour), aes(x = hour, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Waze Acc.")

# by weekday hour
p6 <- ggplot(data = ts_group_by(w.sub_seg, weekday, hour), aes(x = paste0(as.numeric(weekday),"-",hour), y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90)) +
  ylab("Waze Acc.") + xlab("weekday-hour")

# by month hour
p7 <- ggplot(data = ts_group_by(w.sub_seg, month, hour), aes(x = paste0(month,hour), y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90)) +
  ylab("Waze Acc.") + xlab("month-hour")

multiplot(p1, p2, p3, p4, p5, p6, p7)

dev.off()


f <- paste0(visual.loc, '/Bellevue_crashes_time_series_KSI.png')

# use minimal format
theme_set(theme_minimal())

png(f, width = 12, height = 10, units = 'in', res = 300)

# by day hour
p1 <- ggplot(data = w.sub_seg, aes(x = time_hr, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("KSI Crashes")

# by day
p2 <- ggplot(data = ts_group_by(w.sub_seg, date), aes(x = date, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("KSI Crashes")

# by month
p3 <- ggplot(data = ts_group_by(w.sub_seg, month), aes(x = month, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1) + geom_point() +
  ylab("KSI Crashes") +
  scale_x_date(date_breaks = "1 month", date_labels = "%b")

# by week
# p4 <- ggplot(data = ts_group_by(w.sub_seg, week), aes(x = week, y = nCrashKSI)) +
#   geom_line(color = "darkorchid4", size = 1) + geom_point() +
#   ylab("Crashes") +
#   scale_x_date(date_breaks = "1 week", date_labels = "%W")
# by weekday
p4 <- ggplot(data = ts_group_by(w.sub_seg, weekday), aes(x = weekday, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("KSI Crashes")

# by hour
p5 <- ggplot(data = ts_group_by(w.sub_seg,  hour), aes(x = hour, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("KSI Crashes")

# by weekday hour
p6 <- ggplot(data = ts_group_by(w.sub_seg, weekday, hour), aes(x = paste0(as.numeric(weekday),"-",hour), y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90)) +
  ylab("KSI Crashes") + xlab("weekday-hour")

# by month hour
p7 <- ggplot(data = ts_group_by(w.sub_seg, month, hour), aes(x = paste0(month,hour), y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90)) +
  ylab("KSI Crashes") + xlab("month-hour")

multiplot(p1, p2, p3, p4, p5, p6, p7)

dev.off()


# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start Modelings ----
# Start simple
# 01 Base: Road network (seg_var)
# 02: Add FARS only ("nFARS")
# 03: Add Weather only (weather_var)
# 04: Add Waze features from event type (but not the counts of all Waze events together) alert_types, alert_subtypes
# 05: Add all other Waze features (waze_rd_type, waze_dir_travel)


starttime = Sys.time()

# Make a list of variables to include from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

includes = c(
              # waze_rd_type,    # road types from Waze
              # waze_dir_travel, # direction of travel
              # alert_types,     # counts of waze events by alert types
              # alert_subtypes,  # counts of waze events by sub-alert types
              # weather_var,     # Weather variables
              # "nFARS",         # FARS variables
              # "nBikes",        # bike/ped conflict counts at segment level (no hour)
              # "hour",          # hour
              seg_var
          )

modelno = "00"

response.var <- response.var.list[2] # binary data, biCrash

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

# extract logistic model objects, save in a list
model_type = "logistic_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    ClassFilter <- function(x) inherits(get(x), 'glm')
    row.name <- Filter(ClassFilter, ls() )
    logistic_models <- lapply(row.name, function(x) get(x))
    save(list = c("logistic_models"), file = out.name)
    
  }


# 4/3/2019 Todos : 1. scale the numeric columns, and re-run logistic regressions. 2. Lasso; 3. logistic regression - present the coefficients using OR; 4. logistic regression with mixed effects; 5. double check the hour column; 6. aggregate data into multi-hour window; 7. Do a model with just the one hour of data. 8. incoporate day of week, month, hour of day as the temporal indicators; 9. XGBoost
# x <- w.all %>% group_by(hour) %>% summarize(sumCrash = sum(uniqueCrashreports))
# use as reference: https://medium.com/geoai/using-machine-learning-to-predict-car-accident-risk-4d92c91a7d57
# https://gduer.github.io/Collision-Prediction-in-Louisville-KY/#5_model_selection

# once we aggregate to a 4 hour window or a day, the counts might change, then we will need a zero-inflated NB model.


# create a summary table with diagnostics for all the linear model ----
# Get the list of linear models
ClassFilter <- function(x) inherits(get(x), 'lm' ) & !inherits(get(x), 'glm') # excluding other potential classes that contains "lm" as the keywords.
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(output.loc, "Bell_linear_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "/WA/utility/Model_Summary().R"))
    linear_model_summary_list <- linear_model_summary(model_list, out.name)
  }
# save(list = c("linear_model_summary_list"), file = file.path(data.loc, 'Segments', "Bell_linear_model_summary_list.Rdata"))

M <- linear_model_summary_list$M
model_summary  <- linear_model_summary_list$model_summary
model_compare <- linear_model_summary_list$model_compare


# create a summary table with diagnostics for all the logistic model ----
# Get the list of logistic models
ClassFilter <- function(x) inherits(get(x), 'glm')
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(data.loc, 'Segments', "Bell_logistic_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "/WA/utility/Model_Summary().R"))
    logistic_model_summary_list <- logistic_model_summary(model_list, out.name)
  }
# save(list = c("logistic_model_summary_list"), file = file.path(data.loc, 'Segments', "Bell_logistic_model_summary_list.Rdata"))

M <- logistic_model_summary_list$M
model_summary  <- logistic_model_summary_list$model_summary
model_compare <- logistic_model_summary_list$model_compare


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)

