# Analysis of crashes on Bellevue road network segmetns.
# Analysis approaches:
# - Xgboost - Typically model performance is better than other methods, try here
# Test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

#source(file.path(codeloc, 'utility/get_packages.R')) #run if need packages
# load functions with group_by
source(file.path(codeloc, 'WA/utility/visual_fun.R'))

library(caret)
library(tidyverse)
library(ggplot2)
#library(GGally)
library(dplyr)
#library(corrplot)
#library(Amelia)
#library(mlbench)
#library(xts)
library(lubridate)
library(pscl) # zero-inflated Poisson
#library(MASS) # NB model
#library(randomForest) # random forest
#library(car) # to get vif()
library(xgboost)
library(Metrics)
library(DiagrammeR)

# #install from Github (Windows user will need to install Rtools first.)
# install.packages("drat", repos="https://cran.rstudio.com")
# drat:::addRepo("dmlc")
# install.packages("xgboost", repos="http://dmlc.ml/drat/", type = "source")

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
nrow(w.all.yr.seg) # total 1679 rows

# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all.yr.seg), value = T), "year", "wkday", "grp_name", "grp_hr", "nFARS_1217",
               grep("Crash", names(w.all.yr.seg), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all.yr.seg), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. 
colSums(w.all.yr.seg[, waze_rd_type]) #
#All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all.yr.seg), value = T)[-7] 
colSums(w.all.yr.seg[,waze_dir_travel]) # Remove mean MagVar

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS_1217")

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl")
table(w.all.yr.seg$ArterialCl)

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, 
                           "other_var" = other_var, 
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

#Remove ArterialCL levels with no observations
w.all.yr.seg$ArterialCl <- factor(w.all.yr.seg$ArterialCl) 
table(w.all.yr.seg$ArterialCl)

# check missing values and all zero columns ----
# if any other columns are all zeros
all_var <- vector()
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

any(sapply(w.all.yr.seg[, all_var], function(x) all(x == 0))) # Returns False if no columns have all zeros
# all variables of w.all.yr.seg are clean now. None of them are all-zero column. 

# checking missing fields
sum(is.na(w.all.yr.seg[, all_var]))

# Mean and variance ----
hist(w.all.yr.seg$uniqueCrashreports)
mean(w.all.yr.seg$uniqueCrashreports) # 0.8159619
var(w.all.yr.seg$uniqueCrashreports) # 3.041795

# <><><><><><><><><><><><><><><><><><><><><><><><>
# XGBoost ----
# build a list containing two things, label and data

# Stratify random sampling to ensure all road classes are represented in training set
d_s <- w.all.yr.seg %>%
  mutate(train_index = 1:nrow(w.all.yr.seg))%>%
  dplyr::select(train_index, ArterialCl)

samprow <- vector()
train_index = 1:nrow(w.all.yr.seg)

for(i in unique(d_s$ArterialCl)){
  d <- d_s %>%
    filter(ArterialCl == i)
  samprow_ArterialCl <- sample(d$train_index, nrow(d)*.3, replace = F)
  samprow <- c(samprow, samprow_ArterialCl)
}

ValidSet <- w.all.yr.seg[samprow,]
TrainSet <- w.all.yr.seg[!rownames(w.all.yr.seg) %in% samprow,]
PredSet <- rbind(TrainSet, ValidSet)

# ensure sampling number adds up
stopifnot(nrow(ValidSet) == length(samprow))
stopifnot(nrow(ValidSet) + nrow(TrainSet) == nrow(w.all.yr.seg))

#Check 70/30 ratios by group
table(ValidSet$ArterialCl)/table(TrainSet$ArterialCl) #~0.4
table(ValidSet$ArterialCl)/table(TrainSet$ArterialCl) #~0.4
table(ValidSet$ArterialCl)/table(TrainSet$ArterialCl)

#save(list = c('ValidSet', 'TrainSet', 'train_index'), file = 'w.all.yr.seg_train.RData')

#Select only arterials (do this earlier in the code?)
Art.Only <- T # False to use all road class, True to Arterial only roads
if(Art.Only) {TrainSet = TrainSet %>% filter(ArterialCl != "Local")} else {TrainSet = TrainSet}
if(Art.Only) {ValidSet = ValidSet %>% filter(ArterialCl != "Local")} else {ValidSet = ValidSet}
if(Art.Only) {PredSet = PredSet %>% filter(ArterialCl != "Local")} else {PredSet = PredSet}

# need to convert character and factor columns to dummy variables.
continous_var <- c(waze_dir_travel, #waze_rd_type, # direction of travel + road types from Waze
                   alert_types,     # counts of waze events by alert types
                   "nBikes",        # bike/ped conflict counts at segment level (no hour)
                   "nFARS_1217",         # FARS variables
                   "Shape_STLe", "SpeedLimit")

includes = c(
  waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  #alert_subtypes,  # counts of waze alerts by subtype
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS_1217",         # FARS variables
  seg_var
)

includes_xgb <- includes[-which(includes %in% "ArterialCl")]

ArterialCl <- TrainSet$ArterialCl
TrainSet_xgb <- data.frame(TrainSet[, includes_xgb], model.matrix(~ ArterialCl + 0))
table(ArterialCl)

ArterialCl <- ValidSet$ArterialCl
ValidSet_xgb <- data.frame(ValidSet[, includes_xgb], model.matrix(~ ArterialCl + 0))
table(ArterialCl)

ArterialCl <- PredSet$ArterialCl
PredSet_xgb <- data.frame(PredSet[, includes_xgb], model.matrix(~ ArterialCl + 0))
table(ArterialCl)

# XGboost unique crash counts, updated code to test parameters in xgboost function 
# Side note: "You can use an offset in xgboost for Poisson regression, by setting the base_margin value in the xgb.DMatrix object".
# Example:
# xgbMatrix <- xgb.DMatrix(as.matrix(temp2), 
#                         label = Insurance$Claims)
# setinfo(xgbMatrix, "base_margin",log(Insurance$Holders))

response.var <- response.var.list[1] # use crash counts
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

set.seed(1024)
xgb.m <- assign(paste0('m', modelno),
                xgboost(data = dtrain$data, 
                        label = dtrain$label, 
                        max_depth = 4,
                        eta = 0.1, 
                        nthread = 2,
                        nrounds = 50,
                        subsample = 1,
                        #colsample_bytree = 0.9,
                        booster = "gbtree",
                        #eval_metric = "mae",
                        objective="count:poisson",
                        #base_score = 0.56,
                        silent = 1))

y_hat_xgb <- predict(xgb.m, data.matrix(dtest$data %>% as.data.frame))
xgb_mae <- mae(dtest$label, y_hat_xgb)
xgb_mae

pred_train <- predict(xgb.m, dtrain$data)
pred_test <- predict(xgb.m, dtest$data)
pred_pred <- predict(xgb.m, dpred$data)

#xgb.m
importance_matrix <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m)
xgb.plot.importance(importance_matrix[1:20,])

# Add Observed-predicted summaries
trainObsPred <- TrainSet[,response.var] - pred_train
hist(trainObsPred)
plot(TrainSet[,response.var],pred_train)
plot(ValidSet[,response.var],pred_test)
plot(PredSet[,response.var],pred_pred)
importance_matrix <- xgb.importance(feature_names = colnames(dtrain$data), model = xgb.m)
xgb.plot.importance(importance_matrix[1:20,])


# Compare totals
sum(PredSet$uniqueCrashreports)
sum(predict(xgb.m, dpred$data))

# % of variance explained
cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train )^2) /
                                    sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
)
) # 63% using 50 rounds


cat("% Var explained: \n", 100 * (1-sum(( ValidSet[,response.var] - pred_test )^2) /
                                    sum(( ValidSet[,response.var] - mean(PredSet[,response.var]))^2)
)
) # 24.57% using 50 rounds

cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)
) # 49.8% using 50 rounds

par(mfrow=c(1,1))

#pretty plot - how to interpret?
xgb.plot.tree(feature_names = colnames(dtrain$data),model=xgb.m, trees = 0, show_node_id = TRUE)


range(pred_pred) # 0.0005870936 5.9243960381 (eta=.3,50 rounds)
range(PredSet[,response.var]) # 0 - 6

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


#Code to test, not working
#bestRound = which.max(as.matrix(xgb.cv)[,3]-as.matrix(xgb.cv)[,4])
#bestRound

#test <- chisq.test(dtrain$Shape_STLe, output_vector)
#xgb.modsum <- xgb.dump(xgb.m, with_stats = T)
#xgb.modsum[1:10]

#method to set exposure - Poisson assumes a rate, need to scale by segment length (and exposure): 
# https://stackoverflow.com/questions/35660588/xgboost-poisson-distribution-with-varying-exposure-offset
#setinfo(xgtrain, "base_margin", log(d$exposure))



# XGboost unique crash counts, original code for 20 rounds 
response.var <- response.var.list[1] # use crash counts
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
xgb.plot.importance(importance_matrix[1:20,])

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


# XGboost weighted crash counts, original code for 25 rounds 
response.var <- response.var.list[4] # use weighted crash counts
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

modelno = "15.xgb.art.wkend.25rd.weighted.v2"
#modelno = "15.xgb.art.wkend.30rd.weighted.v2" 

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
xgb.plot.importance(importance_matrix[1:20,])


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

#pred_train_weighted <- predict(m15.xgb.art.wkend.30rd.weighted.v2, dtrain$data)
#pred_pred_weighted <- predict(m15.xgb.art.wkend.30rd.weighted.v2, dpred$data)

# todo, double check which model prediction we used in the out file.
# use WeightedCrashes as the response for the XGBoost model, and add the prediction to the out table.

# compare the distribution of response and prediction
f <- paste0(visual.loc, '/Bellevue_hist_obs_pred_xgb_',paste0('m', modelno),'.png')
png(file = f,  width = 6, height = 10, units = 'in', res = 300)
par(mfrow = c(2,1))
hist(log(PredSet[,response.var] + 0.0001), main = "obs") # obs
hist(log(pred_pred_weighted + 0.0001), main = "pred") # prediction
dev.off()

# importance matrix
importance_matrix <- xgb.importance(model = m15.xgb.art.wkend.25rd.weighted.v2)
# xgb.importance(feature_names = colnames(dtrain$data), model = m15.xgb.art.wkend)
print(importance_matrix)

f <- paste0(visual.loc, '/Bellevue_importance_xgb_',paste0('m', modelno),'.png')
png(file = f,  width = 6, height = 10, units = 'in', res = 300)
xgb.plot.importance(importance_matrix = importance_matrix)
dev.off()

# # model output both training and testing errors.
# train <- xgb.DMatrix(data = dtrain$data, label = dtrain$label)
# test <- xgb.DMatrix(data = dtest$data, label = dtest$label)
# pred <- xgb.DMatrix(data = dpred$data, label = dpred$label)
# watchlist <- list(train = train, test = test)
# 
# bst <- xgb.train(data = train, max.depth=2, eta=1, nthread = 2, nrounds=2, watchlist=watchlist, objective = "count:poisson")

# Save xgb model output
model_type = "XGB_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    row.name <- c("m15.xgb.art.wkend.10rd", "m15.xgb.art.wkend.20rd", "m15.xgb.art.wkend.30rd", "m15.xgb.art.wkend.20rd.weighted", "m15.xgb.art.wkend.25rd.weighted", "m15.xgb.art.wkend.25rd.weighted.v2")
    XGB_models <- lapply(row.name, function(x) get(x))
    save(list = c("XGB_models", "row.name", "TrainSet", "ValidSet", "PredSet", "response.var.list", "pred_pred", "includes_xgb"), file = out.name)
    
  }
