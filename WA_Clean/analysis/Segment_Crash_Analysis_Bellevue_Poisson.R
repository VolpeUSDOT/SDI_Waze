# Analysis of crashes on Bellevue road network segmetns.
# Inputs:
# Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData
# Bell_Poi_model_summary_list.Rdata
# 
# Outputs:
# Bell_Poisson_models.Rdata
# Poisson_model_summary_update.csv
#
# Analysis approaches:
# - Poisson
# - Random forests 
# - Xgboost Perhaps, but likely not as useful in this context since we want to interpret coefficients of individual factors 
# But typically model performance is better so worth trying
# In each of these, test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

###Inputs###
#Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData
#Bell_Poi_model_summary_list.Rdata

###Outputs###
#Bell_Poisson_models.Rdata
#Poisson_model_summary_update.csv

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- "~/" #Replace with path to code

#source(file.path(codeloc, 'utility/get_packages.R')) #run if need packages
# load functions with group_by
source(file.path(codeloc, 'utility/visual_fun.R'))

library(tidyverse)
library(ggplot2)
library(dplyr)
library(lubridate)
library(xgboost)
library(DiagrammeR)

# #install from Github (Windows user will need to install Rtools first.)
# install.packages("drat", repos="https://cran.rstudio.com")
# drat:::addRepo("dmlc")
# install.packages("xgboost", repos="http://dmlc.ml/drat/", type = "source")

## Working on shared drive
wazeshareddir <- #replace with path to root directory
data.loc <- #replace with path to data folder in root directory
seg.loc <- file.path(data.loc, "Segments")
output.loc <- file.path(data.loc, "Model_output")
visual.loc <- file.path(data.loc, "Model_visualizations")

setwd(data.loc)

# Load both the 1hr and 4hr data
fn = "Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData"
load(file.path(seg.loc, fn))

# <><><><><><><><><><><><><><><><><><><><><><><><> Start from prepared data for 4 hour window
# Add crash counts per mile for logistic models
w.all$CrashPerMile = as.numeric(w.all$uniqueCrashreports/(w.all$Shape_STLe*0.000189394))

# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all.4hr.wd), value = T), "year", "wkday", "grp_name", "grp_hr", "nFARS_1217",
               grep("Crash", names(w.all.4hr.wd), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all.4hr.wd), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. 
colSums(w.all.4hr.wd[, waze_rd_type]) #All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all.4hr.wd), value = T)[-7] 
colSums(w.all.4hr.wd[,waze_dir_travel]) # Remove mean MagVar

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS_1217")

time_var = c("grp_hr", "wkday", "wkend") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl")
table(w.all.4hr.wd$ArterialCl)

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, 
                           "other_var" = other_var, 
                           "time_var" = time_var, 
                           "weather_var" = weather_var, 
                           "alert_types" = alert_types, 
                           #"alert_subtypes" = alert_subtypes, 
                           #"waze_rd_type" = waze_rd_type, 
                           "waze_dir_travel" = waze_dir_travel
                           )

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports", # number of crashes at each segment at every time segment (every 4 hours at each weekday)
                  "biCrash",            # presence and absence of crash at each segment at every hour of day
                  "nCrashes",            # total crashes at each segment of entire year 2018
                  "WeightedCrashes")    #total crashes weighted by severity (25 for KSI, 10 for injury, 1 for PDO)

# Any last-minute data organization: order the levels of weekday
# create wkend variables using the raw data.
w.all.4hr.wd$wkend = ifelse(w.all.4hr.wd$wkday %in% c("Sunday","Saturday"), "Weekend", "Weekday")

# re-order the factor level (can run only once)
w.all.4hr.wd$wkday = factor(w.all.4hr.wd$wkday, levels(w.all.4hr.wd$wkday)[c(4,2,6,7,5,1,3)])
# create another columns, and rename with short names
w.all.4hr.wd$wkday.s = w.all.4hr.wd$wkday
levels(w.all.4hr.wd$wkday.s) <- list(Sun = "Sunday", 
                                     Mon = "Monday", 
                                     Tue = "Tuesday", 
                                     Wed = "Wednesday", 
                                     Thu = "Thursday", 
                                     Fri = "Friday", 
                                     Sat = "Saturday"
                                     )


#Remove ArterialCL levels with no observations
w.all.4hr.wd$ArterialCl <- factor(w.all.4hr.wd$ArterialCl) 

# check missing values and all zero columns ----
# if any other columns are all zeros
all_var <- vector()
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start Poisson Modelings ----
# Below describes models run
# 10: seg_var + TimeOfDay + DayofWeek + Month
# 11: 10 + nFARS
# 12: 10 + nFARS + nBikes
# 13: {Best of 10 - 12} = 12 + weather_var
# 14: {Best of 10 - 13} = 12 + alert_types
# 15: 14 + waze_dir_travel + waze_rd_type

# Make a list of variables to include from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

includes = c(
  waze_dir_travel,
  waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  # weather_var,     # Weather variables
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS_1217",         # FARS variables
  seg_var, "wkend", "grp_hr"
)

modelno = "15.poi.art.wkend"

response.var <- response.var.list[1] # use crash counts

Art.Only <- T # False to use all road class, True to Arterial only roads
if(Art.Only) {data = w.all.4hr.wd %>% filter(ArterialCl != "Local")} else {data = w.all.4hr.wd} 

# Simple 
assign(paste0('m', modelno),
       glm(use.formula, data = data, family = poisson) # regular Poisson Model
)

# extract logistic model objects, save in a list
model_type = "Poisson_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    ClassFilter <- function(x) inherits(get(x), 'glm')
    row.name <- Filter(ClassFilter, ls() )
    Poisson_models <- lapply(row.name, function(x) get(x))
    save(list = c("Poisson_models", "row.name"), file = out.name)
    
  }

# create a summary table with diagnostics for all the Poisson model ----
# Get the list of glm models
ClassFilter <- function(x) inherits(get(x), 'glm' ) # excluding other potential classes that contains "lm" as the keywords.
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(output.loc, "Bell_Poi_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "utility/Model_Summary().R"))
    Poisson_model_summary_list <- Poisson_model_summary(model_list, out.name)
  }

M <- Poisson_model_summary_list$M
model_summary  <- Poisson_model_summary_list$model_summary
model_compare <- Poisson_model_summary_list$model_compare

write.csv(model_summary, file.path(output.loc, "Poisson_model_summary_update.csv"), row.names = F)
