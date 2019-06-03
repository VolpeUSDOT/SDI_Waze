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

#source(file.path(codeloc, 'utility/get_packages.R')) #run if need packages
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
library(car) # to get vif()
library(xgboost)
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
nrow(w.all.4hr.wd) # total 12,608 rows
table(w.all.4hr.wd$uniqueCrashreports) # ~10% of the data has non-zero counts, 0.8% of the data has counts larger than 1

# With new code (5/29/19)
#    0     1     2     3     4     6 
#11358  1155    75    17     2     1 

#Other variables
table(w.all.4hr.wd$nCrashKSI)
table(w.all.4hr.wd$nCrashInjury)
table(w.all.4hr.wd$nFARS_1217)
table(w.all.4hr.wd$nFARS)
table(w.all.4hr.wd$nCrashes)
table(w.all.4hr.wd$ArterialCl)


#histograms
png(file = paste0(visual.loc, "/histigrams_4hr_wd_MegVar.png"), width = 6, height = 8, units = 'in', res = 300)
par(mfrow = c(2,3))

hist(log(w.all.4hr.wd$nMagVar330to30N), xlab = "nMagVar330to30N", main = "")
hist(log(w.all.4hr.wd$nMagVar30to90NE), xlab = "nMagVar30to90NE", main = "")
hist(log(w.all.4hr.wd$nMagVar90to150SE), xlab = "nMagVar90to150SE", main = "")
hist(log(w.all.4hr.wd$nMagVar150to210S), xlab = "nMagVar150to210S", main = "")
hist(log(w.all.4hr.wd$nMagVar210to270SW), xlab = "nMagVar210to270SW", main = "")
hist(log(w.all.4hr.wd$nMagVar270to330NW), xlab = "nMagVar270to330NW", main = "")

dev.off()


# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all.4hr.wd), value = T), "year", "wkday", "grp_name", "grp_hr", "nFARS_1217",
               grep("Crash", names(w.all.4hr.wd), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all.4hr.wd), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. 
colSums(w.all.4hr.wd[, waze_rd_type]) #
#All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all.4hr.wd), value = T) 
# colSums(w.all.4hr.wd[,waze_dir_travel]) # none of these columns are all zeros.

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS_1217")

time_var = c("grp_hr", "wkday", "wkend") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl")
            # , "FunctionCl"
             # "ArterialCl" is complete. There are 6 rows with missing values in "FunctionCl", therefore if we use in the model, we will lose these rows.
table(w.all.4hr.wd$ArterialCl)

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, 
                           "other_var" = other_var, 
                           "time_var" = time_var, 
                           "weather_var" = weather_var, 
                           "alert_types" = alert_types, 
                           "alert_subtypes" = alert_subtypes, 
                           "waze_rd_type" = waze_rd_type, 
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

# check variables to make sure it is correct
table(w.all.4hr.wd[,c("wkend", "wkday", "wkday.s")])

#Remove ArterialCL levels with no observations
w.all.4hr.wd$ArterialCl <- factor(w.all.4hr.wd$ArterialCl)


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
all_var <- vector()
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

any(sapply(w.all.4hr.wd[, all_var], function(x) all(x == 0))) # Returns False if no columns have all zeros
# all variables of w.all.4hr.wd are clean now. None of them are all-zero column. 

# checking missing fields
sum(is.na(w.all.4hr.wd[, all_var]))

# Check time variables & Time Series visuals ----
stopifnot(length(unique(w.all.4hr.wd$wkday)) == 7)
stopifnot(length(unique(w.all.4hr.wd$grp_hr)) == 6)

## ggplot of time series
f <- paste0(visual.loc, '/Bellevue_WazeAccidents_time_series_4hr_wd.png')

# use minimal format
theme_set(theme_minimal())

png(f, width = 6, height = 10, units = 'in', res = 300)

# by both
p1 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = grp_hr, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("wkday.s") +
  ylab("Waze Acc.")

p2 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = wkday.s, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("grp_hr") +
  ylab("Waze Acc.")

# by wkday
p3 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s), aes(x = wkday.s, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Waze Acc.") +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90))

# by 4-hour window
p4 <- ggplot(data = ts_group_by(w.all.4hr.wd, grp_hr), aes(x = grp_hr, y = nWazeAccident)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("Waze Acc.") 

multiplot(p1, p2, p3, p4)

dev.off()


f <- paste0(visual.loc, '/Bellevue_KSIcrashes_time_series_4hr_wd.png')

# use minimal format
theme_set(theme_minimal())

png(f, width = 6, height = 10, units = 'in', res = 300)

# by both
p1 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = grp_hr, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("wkday.s") +
  ylab("KSI Crashes")

p2 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s, grp_hr), aes(x = wkday.s, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() + facet_wrap("grp_hr") +
  ylab("KSI Crashes")

# by wkday
p3 <- ggplot(data = ts_group_by(w.all.4hr.wd, wkday.s), aes(x = wkday.s, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("KSI Crashes") +
  theme(axis.text.x = element_text(size=7, vjust = 0.2, angle=90))

# by 4-hour window
p4 <- ggplot(data = ts_group_by(w.all.4hr.wd, grp_hr), aes(x = grp_hr, y = nCrashKSI)) +
  geom_line(color = "darkorchid4", size = 1, group = 1) + geom_point() +
  ylab("KSI Crashes") 

multiplot(p1, p2, p3, p4)

dev.off()

# Mean and variance ----
hist(w.all.4hr.wd$uniqueCrashreports)
mean(w.all.4hr.wd$uniqueCrashreports) # 0.10866
var(w.all.4hr.wd$uniqueCrashreports) # 0.1211338
# The data is not overdispersed. Maybe a Poisson model is also appropriate.
ggplot(w.all.4hr.wd, aes(uniqueCrashreports)) + geom_histogram() + scale_x_log10()

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start Poisson Modelings ----
# Start simple
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

( predvars = names(w.all.4hr.wd)[names(w.all.4hr.wd) %in% includes] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       glm(use.formula, data = data, family = poisson) # regular Poisson Model
       # zeroinfl(use.formula, data = data) # zero-inflated Possion
       # glm.nb(use.formula, data = data) # regular NB
       # zeroinfl(use.formula, data = data, dist = "negbin", EM = F) # zero-inflated NB, EM algorithm looks for optimal starting values, which is slower.
)

# Summarize
summary(get(paste0('m', modelno)))

fitted <- fitted(get(paste0('m', modelno)))
pres <- residuals(get(paste0('m', modelno)), type="pearson")
assign(paste0('diag.plot.', modelno), plot((fitted)^(1/2), abs(pres)))

##Note from Erika: These need to be run after the function below to run all models##
AIC(m10.0poi, m10.poi, m10.nb, m10.0nb) # we ran a base model 10, and here is the comparison of AIC.
# model    df      AIC
# m10.0poi 36 8832.233
# m10.poi  18 8882.401
# m10.nb   19 8850.987
# m10.0nb  37 8831.231

# plot(get(paste0('m', modelno)), which=3)

AIC(m10.poi, m11.poi, m12.poi, m13.poi, m14.poi, m15.poi) # all roads, model 12 seems to be slightly better, adding weather does not improve, so will exclude weather out of the model. Model 15 is the best, adding Waze data helps a lot.
# model   df      AIC
# m10.poi 18 8882.401
# m11.poi 19 8884.033
# m12.poi 20 8879.510
# m13.poi 24 8879.688
# m14.poi 24 8615.698
# m15.poi 35 7632.854

AIC(m10.poi.art, m11.poi.art, m12.poi.art, m13.poi.art, m14.poi.art, m15.poi.art) # Arterial only model. model 12 is better. Adding weather improves slightly, so it is included.
#             df      AIC
# m10.poi.art 17 8193.005
# m11.poi.art 18 8194.777
# m12.poi.art 19 8190.248
# m13.poi.art 23 8189.671
# m14.poi.art 23 7940.256
# m15.poi.art 33 7917.516

AIC(m15.poi.art, m15.poi.art.wkend) # compare two models with weekday or weekend as variables.
#Note from Erika: Can't compare these directly - different number of observations
# they should be able to compare, they are both arterial only model with 11,630 observations.
#                   df      AIC
# m15.poi.art       33 7917.516
# m15.poi.art.wkend 28 7915.291

AIC(m10.poi.art.wkend, m11.poi.art.wkend, m12.poi.art.wkend, m13.poi.art.wkend, m14.poi.art.wkend, m15.poi.art.wkend)
#                   df      AIC
# m10.poi.art.wkend 12 8189.240
# m11.poi.art.wkend 13 8191.022
# m12.poi.art.wkend 14 8186.461
# m13.poi.art.wkend 18 8187.136
# m14.poi.art.wkend 18 7938.109
# m15.poi.art.wkend 28 7915.291

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

# row.name <- c("m10.poi.art", "m10.poi.art.wkend" "m11.poi.art", "m11.poi.art.wkend", "m12.poi.art", "m12.poi.art.wkend" "m13.poi.art", "m13.poi.art.wkend", "m14.poi.art", "m14.poi.art.wkend" "m15.poi.art", "m15.poi.art.wkend")

# for (i in 1:length(row.name)) {
#   
#   assign(row.name[i], Poisson_models[[i]])
#          
# }

# create a summary table with diagnostics for all the Poisson model ----
# Get the list of glm models
ClassFilter <- function(x) inherits(get(x), 'glm' ) # excluding other potential classes that contains "lm" as the keywords.
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(output.loc, "Bell_Poi_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "/WA/utility/Model_Summary().R"))
    Poisson_model_summary_list <- Poisson_model_summary(model_list, out.name)
  }

M <- Poisson_model_summary_list$M
model_summary  <- Poisson_model_summary_list$model_summary
model_compare <- Poisson_model_summary_list$model_compare

write.csv(model_summary, file.path(output.loc, "Poisson_model_summary_update.csv"), row.names = F)

continous_var <- c(waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
                   alert_types,     # counts of waze events by alert types
                   weather_var,     # Weather variables
                   "nBikes",        # bike/ped conflict counts at segment level (no hour)
                   "nFARS_1217",         # FARS variables
                   "Shape_STLe", "SpeedLimit")
corr <- cor(w.all.4hr.wd[, c(continous_var, "uniqueCrashreports")])
write.csv(corr, file.path(output.loc, "Correlation.csv"))
vif(m15.poi.art) # there are aliased coefficients in the model

# Lasso or Ridge Regression ----
install.packages("glmnet", repos = "http://cran.us.r-project.org")
library(glmnetUtils)
library(glmnet)
x <- as.matrix(w.all.4hr.wd[, includes])
y <- w.all.4hr.wd[, response.var]
fit = glmnet(x, y, family = "poisson")
fit = glmnet(use.formula, data = data, family = "poisson")
plot(fit)

# Random Forest ----
# https://xgboost.readthedocs.io/en/latest/R-package/xgboostPresentation.html

# convert character to factor for random forest model
w.all.4hr.wd = w.all.4hr.wd %>% mutate_if(is.character, as.factor)

# split data to train and validation sets
set.seed(254)
train_index <- sample(nrow(w.all.4hr.wd), 0.7*nrow(w.all.4hr.wd), replace = FALSE)
TrainSet <- w.all.4hr.wd[train_index,]
ValidSet <- w.all.4hr.wd[-train_index,]
PredSet <- rbind(TrainSet, ValidSet)

# summary(TrainSet)


# 10: seg_var + TimeOfDay + DayofWeek + Month
# 11: 10 + nFARS
# 12: 10 + nFARS + nBikes
# 13: {Best of 10 - 12} = 12 + weather_var
# 14: {Best of 10 - 13} = 12 + alert_types
# 15: 14 + waze_dir_travel + waze_rd_type

# 12 is the best among 10-12, and 10-13
includes = c(
  waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  # weather_var,     # Weather variables
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS_1217",         # FARS variables
  seg_var, "wkend", "grp_hr"
)

modelno = "15.rf.art.wkend"

response.var <- response.var.list[1] # use crash counts

Art.Only <- T # False to use all road class, True to Arterial only roads
if(Art.Only) {TrainSet = TrainSet %>% filter(ArterialCl != "Local")} else {TrainSet = TrainSet}
if(Art.Only) {ValidSet = ValidSet %>% filter(ArterialCl != "Local")} else {ValidSet = ValidSet}
if(Art.Only) {PredSet = PredSet %>% filter(ArterialCl != "Local")} else {PredSet = PredSet}

# Simple 

( predvars = names(w.all.4hr.wd)[names(w.all.4hr.wd) %in% includes] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       randomForest(use.formula, data = TrainSet %>% filter(ArterialCl != "Local"))) 
# random forest model mtry (how many variables to try in each tree) 
# and maxnodes (how deep the tree goes) are influenctial parameters to set for random forest model.

# Warning
# Warning message:
#   In randomForest.default(m, y, ...) :
#   The response has five or fewer unique values.  Are you sure you want to do regression?
importance(m15.rf.art.wkend)   

rf_pred <- predict(m15.rf.art.wkend, PredSet[, includes])
range(rf_pred) # -2.701728e-16  3.258433e+00

f <- paste0(visual.loc, '/Bellevue_importance_rf_m15.rf.art.wkend.png')
png(file = f,  width = 6, height = 10, units = 'in', res = 300)
varImpPlot(m15.rf.art.wkend)
dev.off()

# Compare % of var explained for these models & manual calculation
print(m10.rf.art.wkend) # 2.37, (removing medMagVar, 2.34)
print(m11.rf.art.wkend) # 3.42, (removing medMagVar, 3.28)
print(m12.rf.art.wkend) # 4.13, (removing medMagVar, 3.79)
print(m13.rf.art.wkend) # -0.26 (worst), (removing medMagVar, -0.09)
print(m14.rf.art.wkend) # 32.02, (removing medMagVar, 31.24) Model 14 seems to be better.
print(m15.rf.art.wkend) # 37.04, (removing medMagVar, 29.5)

# the manual calculationg used "pred" within the randomForest object, it is actually OOB predicted values. Because the trees of rf model are grown almost to max depth and will overfit the training set, only cross-validation can be used assess the performance. https://stats.stackexchange.com/questions/109232/low-explained-variance-in-random-forest-r-randomforest
cat("% Var explained: \n", 100 * (1-sum((m15.rf.art.wkend$y-m15.rf.art.wkend$pred   )^2) /
                                  sum((m15.rf.art.wkend$y-mean(m15.rf.art.wkend$y))^2)
                                  )
    ) # 29.53954%

# Save RF model output
model_type = "RF_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))

if(file.exists(out.name)){
  load(out.name)} else {
    
    ClassFilter <- function(x) inherits(get(x), 'randomForest')
    row.name <- Filter(ClassFilter, ls() )
    RF_models <- lapply(row.name, function(x) get(x))
    save(list = c("RF_models", "row.name", "TrainSet", "ValidSet", "PredSet", "response.var"), file = out.name)
    
  }

# Save RF model summary
ClassFilter <- function(x) inherits(get(x), 'randomForest' ) # excluding other potential classes that contains "lm" as the keywords.
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(output.loc, "Bell_RF_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "/WA/utility/Model_Summary().R"))
    RF_model_summary_list <- RF_model_summary(model_list, out.name)
  }

M <- RF_model_summary_list$M
model_summary  <- RF_model_summary_list$model_summary
model_compare <- RF_model_summary_list$model_compare
write.csv(model_summary, file.path(output.loc, "RF_model_summary.csv"), row.names = F)

# XGBoost ----
# build a list containing two things, label and data
set.seed(254)
train_index <- sample(nrow(w.all.4hr.wd), 0.7*nrow(w.all.4hr.wd), replace = FALSE)
TrainSet <- w.all.4hr.wd[train_index,]
ValidSet <- w.all.4hr.wd[-train_index,]
PredSet <- rbind(TrainSet, ValidSet)

Art.Only <- T # False to use all road class, True to Arterial only roads
if(Art.Only) {TrainSet = TrainSet %>% filter(ArterialCl != "Local")} else {TrainSet = TrainSet}
if(Art.Only) {ValidSet = ValidSet %>% filter(ArterialCl != "Local")} else {ValidSet = ValidSet}
if(Art.Only) {PredSet = PredSet %>% filter(ArterialCl != "Local")} else {PredSet = PredSet}

# need to convert character and factor columns to dummy variables.
continous_var <- c(waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
                   alert_types,     # counts of waze events by alert types
                   weather_var,     # Weather variables
                   "nBikes",        # bike/ped conflict counts at segment level (no hour)
                   "nFARS_1217",         # FARS variables
                   "Shape_STLe", "SpeedLimit")
# response.var <- response.var.list[1] # use crash counts
response.var <- response.var.list[4] # use weighted crash

includes = c(
  waze_dir_travel, waze_rd_type, # direction of travel + road types from Waze
  alert_types,     # counts of waze events by alert types
  # weather_var,     # Weather variables
  "nBikes",        # bike/ped conflict counts at segment level (no hour)
  "nFARS_1217",         # FARS variables
  seg_var, "wkend", "grp_hr"
)

includes_xgb <- includes[-which(includes %in% c("ArterialCl", "wkend", "grp_hr"))] #From Erika - which variables are we trying to remove here? Columns have changed

ArterialCl <- TrainSet$ArterialCl
wkend <- TrainSet$wkend
grp_hr <- TrainSet$grp_hr
TrainSet_xgb <- data.frame(TrainSet[, includes_xgb], model.matrix(~ ArterialCl + 0), model.matrix(~ wkend + 0), model.matrix(~ grp_hr + 0))

ArterialCl <- ValidSet$ArterialCl
wkend <- ValidSet$wkend
grp_hr <- ValidSet$grp_hr
ValidSet_xgb <- data.frame(ValidSet[, includes_xgb], model.matrix(~ ArterialCl + 0), model.matrix(~ wkend + 0), model.matrix(~ grp_hr + 0))

ArterialCl <- PredSet$ArterialCl
wkend <- PredSet$wkend
grp_hr <- PredSet$grp_hr
PredSet_xgb <- data.frame(PredSet[, includes_xgb], model.matrix(~ ArterialCl + 0), model.matrix(~ wkend + 0), model.matrix(~ grp_hr + 0))

dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
dtrain <- list("data" = as.matrix(TrainSet_xgb), "label" = TrainSet[,response.var])
dtest <- list("data" = as.matrix(ValidSet_xgb), "label" = ValidSet[,response.var])

modelno = "15.xgb.art.wkend.25rd.weighted.v2"
# modelno = "15.xgb.art.wkend.20rd"
assign(paste0('m', modelno),
       xgboost(data = dtrain$data, label = dtrain$label, max.depth = 6, eta = 1, nthread = 2, nrounds = 25, objective = "count:poisson"))

pred_train <- predict(m15.xgb.art.wkend.20rd, dtrain$data)
pred_test <- predict(m15.xgb.art.wkend.20rd, dtest$data)
pred_pred <- predict(m15.xgb.art.wkend.20rd, dpred$data)
pred_train_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dtrain$data)
pred_pred_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dpred$data)
range(pred_pred) # 0.00407875 4.49043274 ( 10 rounds)  0.0009638735 7.5567440987 (20 rounds) 0-11 (30 rounds) 
range(pred_pred_weighted) # .002860187 23.191209793 (20 rounds weighted crash) 0.001295544 27.112457275 (25 rounds)
range(PredSet[,response.var]) # 0 - 6 (weighted crashes: 0-27)
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

# % of variance explained
cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train )^2) /
                                    sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
)
) # 50.17904% using 10 rounds, 58.09369% using 20 rounds, 65.28416% of Var explained using 30 rounds


cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)
) # 43.23524% using 10 rounds, 44.31% of Var explained using 20 rounds, 41.85474% using 30 rounds.

cat("% Var explained: \n", 100 * (1-sum(( TrainSet[,response.var] - pred_train_weighted )^2) /
                                    sum(( TrainSet[,response.var] - mean(TrainSet[,response.var]))^2)
)
) # 72.00288% (25 rounds, re-run 6/3)

cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred_weighted )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)
) #  28.70593% (20 rounds) 29.03364% (25 rounds)

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

# TODO: 
# next step:
# save model summary for random forest model -- Done
# run XGboost models
# save summary for XGboost models
# a table for the best models of Poisson, RF, and XGboost.
