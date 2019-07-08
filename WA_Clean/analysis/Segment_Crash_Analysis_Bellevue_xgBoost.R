# Analysis of crashes on Bellevue road network segmetns.
# Analysis approaches:
# - Xgboost - Typically model performance is better than other methods, try here
# Test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- "~/" #replace with location of code


# load functions with group_by
source(file.path(codeloc, 'WA/utility/visual_fun.R'))

library(tidyverse)
library(ggplot2)
library(dplyr)
library(lubridate)
library(pscl)
library(xgboost)
library(DiagrammeR)
library(Metrics) # for mae()

## Working on shared drive
wazeshareddir <- #replace with path to root directory
data.loc <- #replace with path to data folder in root directory
seg.loc <- file.path(data.loc, "Segments")
output.loc <- file.path(data.loc, "Model_output")
visual.loc <- file.path(data.loc, "Model_visualizations")

setwd(data.loc)

SHOW_ALL_OUTPUT <- FALSE #whether to display plots

# Load both the 1hr and 4hr data
fn = "Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData"
load(file.path(seg.loc, fn))

# <><><><><><><><><><><><><><><><><><><><><><><><> Start from prepared data for 4 hour window
# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all.4hr.wd), value = T), "year", "wkday", "grp_name", "grp_hr", "nFARS_1217",
               grep("Crash", names(w.all.4hr.wd), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all.4hr.wd), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. 

waze_dir_travel = grep("MagVar", names(w.all.4hr.wd), value = T)[-7] 

waze_dir_travel_cat = "medTravDir"

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS_1217")

time_var = c("grp_hr", "wkday", "wkend") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl")

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, 
                           "other_var" = other_var, 
                           "time_var" = time_var, 
                           "weather_var" = weather_var, 
                           "alert_types" = alert_types, 
                           "alert_subtypes" = alert_subtypes, 
                           "waze_rd_type" = waze_rd_type, 
                           "waze_dir_travel" = waze_dir_travel,
                           "waze_dir_travel_cat" = waze_dir_travel_cat
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

# Check time variables & Time Series visuals ----
stopifnot(length(unique(w.all.4hr.wd$wkday)) == 7)
stopifnot(length(unique(w.all.4hr.wd$grp_hr)) == 6)

# Mean and variance ----
if (SHOW_ALL_OUTPUT){
  hist(w.all.4hr.wd$uniqueCrashreports)
  mean(w.all.4hr.wd$uniqueCrashreports)
  var(w.all.4hr.wd$uniqueCrashreport)
}


# <><><><><><><><><><><><><><><><><><><><><><><><>
# XGBoost ----
# build a list containing two things, label and data

# Stratify random sampling to ensure all times are represented in training set
w.all.4hr.wd <- w.all.4hr.wd %>%
  mutate(wd_tod = paste(wkday,grp_name))

d_s <- w.all.4hr.wd %>%
  mutate(train_index = 1:nrow(w.all.4hr.wd)) %>%
  dplyr::select(train_index, wd_tod, wkday, grp_name)

samprow <- vector()
train_index = 1:nrow(w.all.4hr.wd)

# Randomly sample for each combination of weekday and time period
for(i in unique(d_s$wd_tod)){
  d <- d_s %>%
    filter(wd_tod == i)
  set.seed(256) # set seads so the data is retrievable
  samprow_wd_tod <- sample(d$train_index, nrow(d)*.3, replace = F)
  samprow <- c(samprow, samprow_wd_tod)
}

ValidSet <- w.all.4hr.wd[samprow,]
TrainSet <- w.all.4hr.wd[!rownames(w.all.4hr.wd) %in% samprow,]
PredSet <- rbind(TrainSet, ValidSet)

# ensure sampling number adds up
stopifnot(nrow(ValidSet) == length(samprow))
stopifnot(nrow(ValidSet) + nrow(TrainSet) == nrow(w.all.4hr.wd))

#Select only arterials
Art.Only <- T # False to use all road class, True to Arterial only roads
if(Art.Only) {TrainSet = TrainSet %>% filter(ArterialCl != "Local")} else {TrainSet = TrainSet}
if(Art.Only) {ValidSet = ValidSet %>% filter(ArterialCl != "Local")} else {ValidSet = ValidSet}
if(Art.Only) {PredSet = PredSet %>% filter(ArterialCl != "Local")} else {PredSet = PredSet}


# Prep data for XGboost ----
# need to convert character and factor columns to dummy variables.
continous_var <- c(waze_dir_travel, 
                   waze_rd_type, # direction of travel + road types from Waze
                   alert_types,     # counts of waze events by alert types
                   weather_var,     # Weather variables
                   "nBikes",        # bike/ped conflict counts at segment level (no hour)
                   "nFARS_1217",         # FARS variables
                   "Shape_STLe", "SpeedLimit")

includes = c(
  waze_dir_travel,
  #waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  #alert_subtypes,  # counts of waze alerts by subtype
  #weather_var,     # Weather variables
  #"nBikes",        # bike/ped conflict counts at segment level (no hour)
  #"nFARS_1217",         # FARS variables
  seg_var, 
  "wkend", 
  "grp_hr", 
  "medTravDir"
)

includes_xgb <- includes[-which(includes %in% c("ArterialCl","grp_hr","wkend", "medTravDir"))] #,  

ArterialCl <- TrainSet$ArterialCl
wkend <- TrainSet$wkend
grp_hr <- TrainSet$grp_hr
medTravDir <- TrainSet$medTravDir
TrainSet_xgb <- data.frame(TrainSet[, includes_xgb], 
                           model.matrix(~ ArterialCl + 0), 
                           model.matrix(~ wkend + 0), 
                           model.matrix(~ grp_hr + 0), 
                           model.matrix(~ medTravDir + 0)
                           )

ArterialCl <- ValidSet$ArterialCl
wkend <- ValidSet$wkend
grp_hr <- ValidSet$grp_hr
medTravDir <- ValidSet$medTravDir
ValidSet_xgb <- data.frame(ValidSet[, includes_xgb], 
                           model.matrix(~ ArterialCl + 0), 
                           model.matrix(~ wkend + 0), 
                           model.matrix(~ grp_hr + 0), 
                           model.matrix(~ medTravDir + 0)
                           )

ArterialCl <- PredSet$ArterialCl
wkend <- PredSet$wkend
grp_hr <- PredSet$grp_hr
medTravDir <- PredSet$medTravDir
PredSet_xgb <- data.frame(PredSet[, includes_xgb], 
                          model.matrix(~ ArterialCl + 0), 
                          model.matrix(~ wkend + 0), 
                          model.matrix(~ grp_hr + 0), 
                          model.matrix(~ medTravDir + 0)
                          )

# XGboost unique crash counts
response.var <- response.var.list[1] # use crash counts
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

n.rds = 200
n.eta=.1
m.depth=6
modelno = paste("20.xgb.art.wkend.",n.rds,"rd.","depth",m.depth, sep='')

set.seed(1024)
xgb.m <- assign(paste0('m', modelno),
                xgboost(data = dtrain$data, 
                        label = dtrain$label, 
                        max.depth = m.depth, #default=6 
                        min_child_weight=1, #default = 1, range 0 to inf, min sum of instance weight needed in a child start with 1/sqrt(eventrate)
                        gamma=0, #default=0, minimum loss reduction to make further partition
                        eta = n.eta, #default = 0.3 (0 to 1 range, smaller prevents overfitting)
                        subsample=1, #default = 1, subsample ratio of the training instance
                        colsample_bytree=1, #default=1, range 0 to 1, subsample ratio of columns when constructing each tree (.3-.5)
                        max_delta_step=0, #default 0, range 0 to inf, max delta step we allow each tree's weight estimation to be
                        seed=1,
                        nthread = 2, 
                        nrounds = n.rds,
                        booster = "gbtree",
                        eval_metric = "mae",
                        objective = "count:poisson"))

pred_train <- predict(xgb.m, dtrain$data)
pred_test <- predict(xgb.m, dtest$data)
pred_pred <- predict(xgb.m, dpred$data)

xgb_mae <- mae(dtest$label, pred_test)

#xgb.m

importance_matrix <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m)
if(SHOW_ALL_OUTPUT){
  par(mfrow=c(2,1))
}
if(SHOW_ALL_OUTPUT){
  xgb.plot.importance(importance_matrix[1:20,])
}
if(SHOW_ALL_OUTPUT){
  xgb.plot.tree(feature_names = colnames(dtest$data),model=xgb.m, trees = 0, show_node_id = TRUE)
}



# Add Observed-predicted summaries
trainObsPred <- TrainSet[,response.var] - pred_train
if (SHOW_ALL_OUTPUT){
  hist(trainObsPred)
}
if (SHOW_ALL_OUTPUT){
  plot(TrainSet[,response.var],pred_train)
}
if (SHOW_ALL_OUTPUT){
  plot(ValidSet[,response.var],pred_test)
}
if (SHOW_ALL_OUTPUT){
  plot(PredSet[,response.var],pred_pred)
}


# Compare totals
if (SHOW_ALL_OUTPUT){
  sum(PredSet$uniqueCrashreports)
  sum(PredSet$WeightedCrashes)
  sum(predict(xgb.m, dpred$data))
  
  # Compare maxs
  max(PredSet$uniqueCrashreports)
  max(PredSet$WeightedCrashes)
  max(predict(xgb.m, dpred$data))
  
  # % of variance explained
  cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train )^2) /
                                      sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
  )
  ) # 78.15688%
  
  
  cat("% Var explained: \n", 100 * (1-sum(( ValidSet[,response.var] - pred_test )^2) /
                                      sum(( ValidSet[,response.var] - mean(ValidSet[,response.var]))^2)
  )
  ) # 40.33319%
  
  cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred )^2) /
                                      sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
  )
  ) # 66.78816%
}


# <><><><><><><><><><><><><><><><><><><><><><><><>
# Weighted XGBoost ----

includes = c(
  waze_dir_travel,
  #waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  alert_subtypes,  # counts of waze alerts by subtype
  #weather_var,     # Weather variables
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS_1217",         # FARS variables
  seg_var, 
  "wkend", 
  "grp_hr", 
  "medTravDir"
)

includes_xgb <- includes[-which(includes %in% c("ArterialCl", "grp_hr","wkend", "medTravDir"))] #, 

ArterialCl <- TrainSet$ArterialCl
wkend <- TrainSet$wkend
grp_hr <- TrainSet$grp_hr
medTravDir <- TrainSet$medTravDir
TrainSet_xgb <- data.frame(TrainSet[, includes_xgb], 
                           model.matrix(~ ArterialCl + 0), 
                           model.matrix(~ wkend + 0), 
                           model.matrix(~ grp_hr + 0), 
                           model.matrix(~ medTravDir + 0)
)

ArterialCl <- ValidSet$ArterialCl
wkend <- ValidSet$wkend
grp_hr <- ValidSet$grp_hr
medTravDir <- ValidSet$medTravDir
ValidSet_xgb <- data.frame(ValidSet[, includes_xgb], 
                           model.matrix(~ ArterialCl + 0), 
                           model.matrix(~ wkend + 0), 
                           model.matrix(~ grp_hr + 0), 
                           model.matrix(~ medTravDir + 0)
)

ArterialCl <- PredSet$ArterialCl
wkend <- PredSet$wkend
grp_hr <- PredSet$grp_hr
medTravDir <- PredSet$medTravDir
PredSet_xgb <- data.frame(PredSet[, includes_xgb], 
                          model.matrix(~ ArterialCl + 0), 
                          model.matrix(~ wkend + 0), 
                          model.matrix(~ grp_hr + 0), 
                          model.matrix(~ medTravDir + 0)
)

response.var <- response.var.list[4] # use weighted crash counts
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

n.rds = 400
n.eta=.1
m.depth=4
modelno = paste("21.xgb.art.wkend.weighted.",n.rds,"rd.","depth",m.depth, sep='')

set.seed(1024)
xgb.m <- assign(paste0('m', modelno),
                xgboost(data = dtrain$data, 
                        label = dtrain$label, 
                        max.depth = m.depth, #default=6 
                        min_child_weight=1, #default = 1, range 0 to inf, min sum of instance weight needed in a child start with 1/sqrt(eventrate)
                        gamma=0, #default=0, minimum loss reduction to make further partition
                        eta = n.eta, #default = 0.3 (0 to 1 range, smaller prevents overfitting)
                        subsample=1, #default = 1, subsample ratio of the training instance
                        colsample_bytree=1, #default=1, range 0 to 1, subsample ratio of columns when constructing each tree (.3-.5)
                        max_delta_step=0, #default 0, range 0 to inf, max delta step we allow each tree's weight estimation to be
                        seed=1,
                        nthread = 2, 
                        nrounds = n.rds,
                        booster = "gbtree",
                        eval_metric = "mae",
                        objective = "count:poisson"))

pred_train_weighted <- predict(xgb.m, dtrain$data)
pred_test_weighted <- predict(xgb.m, dtest$data)
pred_pred_weighted <- predict(xgb.m, dpred$data)

xgb_mae_weighted <- mae(dtest$label, pred_test_weighted)

#xgb.m
importance_matrix_weighted <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m)
if(SHOW_ALL_OUTPUT){
  xgb.plot.importance(importance_matrix_weighted[1:20,])
}



# Add Observed-predicted summaries
trainObsPred <- TrainSet[,response.var] - pred_train_weighted
if(SHOW_ALL_OUTPUT){
  hist(trainObsPred)
}
if(SHOW_ALL_OUTPUT){
  plot(TrainSet[,response.var],pred_train_weighted)
}
if(SHOW_ALL_OUTPUT){
  plot(ValidSet[,response.var],pred_test_weighted)
}
if(SHOW_ALL_OUTPUT){
  plot(PredSet[,response.var],pred_pred_weighted)
}

if(SHOW_ALL_OUTPUT){
  # Compare totals
  sum(PredSet$uniqueCrashreports) # 1274
  sum(PredSet$WeightedCrashes) # 4229
  sum(predict(xgb.m, dpred$data)) # 4007
  
  # Compare maxs
  max(PredSet$uniqueCrashreports) # 6
  max(PredSet$WeightedCrashes) # 27
  max(predict(xgb.m, dpred$data)) # 26.982
  
  # % of variance explained
  cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train_weighted )^2) /
                                      sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
  )
  ) # 81%
  
  
  cat("% Var explained: \n", 100 * (1-sum(( ValidSet[,response.var] - pred_test_weighted )^2) /
                                      sum(( ValidSet[,response.var] - mean(ValidSet[,response.var]))^2)
  )
  ) # 12.93513%
  
  cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred_weighted )^2) /
                                      sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
  )
  ) # 63.47352%  
}


# Save xgb model output
model_type = "XGB_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,"061419.Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    row.name <- c("m20.xgb.art.wkend.200rd.depth6", "m21.xgb.art.wkend.weighted.400rd.depth4")
    XGB_models <- lapply(row.name, function(x) get(x))
    save(list = c("XGB_models", "row.name", "TrainSet", "ValidSet", "PredSet", "response.var.list", "pred_pred", "includes_xgb"), file = out.name)
    
  }


# data with XGBoost model predictions
out <- cbind(PredSet, "Xgb_Pred" = pred_pred, "Xgb_Pred_Weighted" = pred_pred_weighted)
write.csv(out, file.path(data.loc, 'Model_output', paste0("Bell_",model_type,"_pred061419.csv")), row.names = F)


# Extra XGboost ----
if(SHOW_ALL_OUTPUT){
  par(mfrow=c(1,1)) 
  range(pred_pred)
  range(PredSet[,response.var])
}

#cross-validation function (xgb.cv) - can use to select features
set.seed(1024)
xgb.cv <- assign(paste0('m.cv', modelno), 
                 xgb.cv(data = dpred$data, 
                        label = dpred$label, 
                        max.depth = m.depth, #default=6 
                        min_child_weight=1, #default = 1, range 0 to inf, min sum of instance weight needed in a child
                        gamma=1, #default=0, minimum loss reduction to make further partition
                        eta = n.eta, #default = 0.3 (0 to 1 range, smaller prevents overfitting)
                        subsample=1, #default = 1, subsample ratio of the training instance
                        colsample_bytree=1, #default=1, range 0 to 1, subsample ratio of columns when constructing each tree
                        max_delta_step=0, #default 0, range 0 to inf, max delta step we allow each tree's weight estimation to be
                        seed=1,
                        nthread = 1, 
                        nrounds = n.rds, 
                        nfold=5,
                        early.stopping.rounds = 3,
                        objective = "count:poisson"))

# XGboost unique crash counts
response.var <- response.var.list[1]
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

modelno = "15.xgb.art.wkend.20rd"

xgb.m1 <- assign(paste0('m', modelno),
                 xgboost(data = dtrain$data, 
                         label = dtrain$label, 
                         max.depth = 6, 
                         eta = 1, 
                         nthread = 2, 
                         nrounds = 20, 
                         objective = "count:poisson"))

pred_train <- predict(m15.xgb.art.wkend.20rd, dtrain$data)
pred_test <- predict(m15.xgb.art.wkend.20rd, dtest$data)
pred_pred <- predict(m15.xgb.art.wkend.20rd, dpred$data)

#xgb.m
importance_matrix <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m1)
if(SHOW_ALL_OUTPUT){
  xgb.plot.importance(importance_matrix[1:20,])
}

if(SHOW_ALL_OUTPUT){
  range(pred_pred) # 0.00407875 4.49043274 ( 10 rounds)  0.0009638735 7.5567440987 (20 rounds) 0-11 (30 rounds) 
  range(PredSet[,response.var]) # 0 - 6 (weighted crashes: 0-27)
  
  # % of variance explained
  cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train )^2) /
                                      sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
  )
  ) # 50.17904% using 10 rounds, 58.09369% using 20 rounds, 65.28416% of Var explained using 30 rounds
  
  
  cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred )^2) /
                                      sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
  )
  ) # 43.23524% using 10 rounds, 44.31% of Var explained using 20 rounds, 41.85474% using 30 rounds.
}

# XGboost weighted crash counts, original code for 25 rounds 
response.var <- response.var.list[4] # use weighted crash counts
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

modelno = "15.xgb.art.wkend.25rd.weighted.v2"

xgb.m2 <- assign(paste0('m', modelno),
                 xgboost(data = dtrain$data, 
                         label = dtrain$label, 
                         max.depth = 6, 
                         eta = 1, 
                         nthread = 2, 
                         nrounds = 25, 
                         objective = "count:poisson"))

pred_train_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dtrain$data)
pred_test_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dtest$data)
pred_pred_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dpred$data)

#xgb.m2
importance_matrix <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m2)
if(SHOW_ALL_OUTPUT){
  xgb.plot.importance(importance_matrix[1:20,])
}


if(SHOW_ALL_OUTPUT){
  range(pred_pred) # 0.00407875 4.49043274 ( 10 rounds)  0.0009638735 7.5567440987 (20 rounds) 0-11 (30 rounds) 
  range(PredSet[,response.var]) # 0 - 6 (weighted crashes: 0-27)
  range(pred_pred_weighted) # .002860187 23.191209793 (20 rounds weighted crash) 0.001555491 21.812366486 (25 rounds) .000816789 24.28149 (30 rounds)
  
  cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train_weighted )^2) /
                                      sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
  )
  ) # 72.00288% (25 rounds, re-run 6/3)
  
  cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred_weighted )^2) /
                                      sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
  )
  ) #  55.71285% (25 rounds)
}

# compare the distribution of response and prediction
f <- paste0(visual.loc, '/Bellevue_hist_obs_pred_xgb_',paste0('m', modelno),'.png')
png(file = f,  width = 6, height = 10, units = 'in', res = 300)
par(mfrow = c(2,1))
hist(log(PredSet[,response.var] + 0.0001), main = "obs") # obs
hist(log(pred_pred_weighted + 0.0001), main = "pred") # prediction
dev.off()

# importance matrix
importance_matrix <- xgb.importance(model = m15.xgb.art.wkend.25rd.weighted.v2)
if(SHOW_ALL_OUTPUT){
  print(importance_matrix)
}

f <- paste0(visual.loc, '/Bellevue_importance_xgb_',paste0('m', modelno),'.png')
png(file = f,  width = 6, height = 10, units = 'in', res = 300)
xgb.plot.importance(importance_matrix = importance_matrix)
dev.off()

# Save xgb model output
model_type = "XGB_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    row.name <- c("m15.xgb.art.wkend.10rd", "m15.xgb.art.wkend.20rd", "m15.xgb.art.wkend.30rd", "m15.xgb.art.wkend.20rd.weighted", "m15.xgb.art.wkend.25rd.weighted", "m15.xgb.art.wkend.25rd.weighted.v2")
    XGB_models <- lapply(row.name, function(x) get(x))
    save(list = c("XGB_models", "row.name", "TrainSet", "ValidSet", "PredSet", "response.var.list", "pred_pred", "includes_xgb"), file = out.name)
    
  }
