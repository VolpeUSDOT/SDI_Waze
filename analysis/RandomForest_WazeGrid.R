# Random Forest work for Waze

# Goals: produce random forest analysis for features of Waze accidents and other event types that predict matching with EDT reports 

# Running on gridded data, with Waze predictors only. Try three versions. For each one, produce confusion matrix, binary model diagnostics, and output .csv of grid IDs and four columns, one each for true negative, false postitive, false negative, true positive.

# 1. April, 70/30 split
# 2. April + May, 70 / 30 split
# 3. April + May, predict June.

# After this is complete, repeat these three with Waze accidents and nMatchEDT_acc as response only
# If only Waze accident report
# 1. Predict to nMatchEDT_buffer_acc
# 2. Subset to only nWazeAccident > 1 ? look at this. 

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

#library(rpart)
library(randomForest)
library(maptree) # for better graphing
#library(party)
#library(partykit)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel


# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal"), dep = T)

setwd("~/")
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} 
if(length(grep("EASdocs", getwd())) > 0) {mappeddrive = "S:"} 
# mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")
codeloc <- "~/git/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

setwd(wazedir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Read in data, renaming data files by month. For each month, prep time and response variables

load("WazeTimeEdtHex_04.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
# Going to binary:
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)

w.04 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeTimeEdtHex_05.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)

w.05 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeTimeEdtHex_06.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)

w.06 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 


# Exploration of response variable: For April, only 1,600 out of 310,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater. 15,000 have 1.
# summary(w.05$nMatchEDT_buffer > 1)
# table(w.05$nMatchEDT_buffer[w.05$nMatchEDT_buffer > 1])


# Analysis ----

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well

fitvars <- names(w.04)[is.na(match(names(w.04),
                                             c("GRID_ID", "day", # place variables to omit as predictors in this vector 
                                               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                                               grep("EDT", names(w.04), value = T)
                                               )))]

# Unnecessary now: all rows are complete cases
# fitdat.04 <- w.04[complete.cases(w.04[,fitvars]),]
# fitdat.05 <- w.05[complete.cases(w.05[,fitvars]),]
# fitdat.06 <- w.06[complete.cases(w.06[,fitvars]),]

wazeformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                              "MatchEDT_buffer"))], 
                           response = "MatchEDT_buffer")


# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Random forest parallel ----
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# On small job, user time faster but system time greater. On a large job, almost exactly ncore X faster
(avail.cores <- parallel::detectCores()) # 4 on local
ntree.use = avail.cores * 100 


# Model 1: April, 70/30 ----
# approx 2.5 min to run on 218k rows of training data with 4 cores

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]
 
# length(testrows) + length(trainrows) == nrow(w.04)

system.time(rf.04 <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                .combine = combine,
                .packages = "randomForest") %dopar%
          randomForest(wazeformula,
               data = w.04[trainrows,],
               ntree = ntree,
               nodesize = 5,
               mtry = 9)
  )


system.time(rf.04.pred <- predict(rf.04, w.04[testrows, fitvars]))

Nobs <- data.frame(t(c(nrow(w.04),
               summary(w.04$MatchEDT_buffer),
               length(w.04$nWazeAccident[w.04$nWazeAccident>0])
               )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred))
bin.mod.diagnostics(predtab)

# save output predictions

out.04 <- data.frame(w.04[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], rf.04.pred)
out.04$day <- as.numeric(out.04$day)
names(out.04)[4:5] <- c("Obs", "Pred")

out.04 = data.frame(out.04,
                    TN = out.04$Obs == 0 &  out.04$Pred == 0,
                    FP = out.04$Obs == 0 &  out.04$Pred == 1,
                    FN = out.04$Obs == 1 &  out.04$Pred == 0,
                    TP = out.04$Obs == 1 &  out.04$Pred == 1)
write.csv(out.04,
          file = "RandomForest_pred_04.csv",
          row.names = F)
                    
varImpPlot(rf.04) # variable imporatance: mean decrease in Gini impurity for this predictor across all trees.  

save(list = c("rf.04",
              "rf.04.pred",
              "testrows",
              "trainrows",
              "w.04",
              "out.04"),
     file = "RandomForest_Output_04.RData")

# Model 2: April + May, 70/30 ----
# ~ 7 min to run on 460k rows training data with 4 cores

w.0405 <- rbind(w.04, w.05)

trainrows <- sort(sample(1:nrow(w.0405), size = nrow(w.0405)*.7, replace = F))
testrows <- (1:nrow(w.0405))[!1:nrow(w.0405) %in% trainrows]

system.time(rf.0405 <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                             .combine = combine,
                             .packages = "randomForest") %dopar%
              randomForest(wazeformula,
                           data = w.0405[trainrows,],
                           ntree = ntree,
                           nodesize = 5,
                           mtry = 9)
)


system.time(rf.0405.pred <- predict(rf.0405, w.0405[testrows, fitvars]))

Nobs <- data.frame(t(c(nrow(w.0405),
                       summary(w.0405$MatchEDT_buffer),
                       length(w.0405$nWazeAccident[w.0405$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.0405$MatchEDT_buffer[testrows], rf.0405.pred))
bin.mod.diagnostics(predtab)

# save output predictions

out.0405 <- data.frame(w.0405[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], rf.0405.pred)
out.0405$day <- as.numeric(out.0405$day)

names(out.0405)[4:5] <- c("Obs", "Pred")

out.0405 = data.frame(out.0405,
                    TN = out.0405$Obs == 0 &  out.0405$Pred == 0,
                    FP = out.0405$Obs == 0 &  out.0405$Pred == 1,
                    FN = out.0405$Obs == 1 &  out.0405$Pred == 0,
                    TP = out.0405$Obs == 1 &  out.0405$Pred == 1)
write.csv(out.0405,
          file = "RandomForest_pred_0405.csv",
          row.names = F)

save(list = c("rf.0405",
              "rf.0405.pred",
              "testrows",
              "trainrows",
              "w.0405",
              "out.0405"),
     file = "RandomForest_Output_0405.RData")

varImpPlot(rf.0405) # variable importance plot

# Model 3: April + May, predict June
# ~ X min to run on 657k rows training data on 4 cores

system.time(rf.0405.all <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                               .combine = combine,
                               .packages = "randomForest") %dopar%
              randomForest(wazeformula,
                           data = w.0405,
                           ntree = ntree,
                           nodesize = 5,
                           mtry = 9)
)


system.time(rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars]))

w.040506 <- rbind(w.0405, w.06)

Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))
bin.mod.diagnostics(predtab)


# save output predictions

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)

names(out.06)[4:5] <- c("Obs", "Pred")

out.06 = data.frame(out.06,
                      TN = out.06$Obs == 0 &  out.06$Pred == 0,
                      FP = out.06$Obs == 0 &  out.06$Pred == 1,
                      FN = out.06$Obs == 1 &  out.06$Pred == 0,
                      TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = "RandomForest_pred_0405_06.csv",
          row.names = F)

save(list = c("rf.0405.all",
              "rf.0405.06.pred",
              "w.06",
              "out.06"),
     file = "RandomForest_Output_0405_06.RData")

varImpPlot(rf.0405.all) # variable importance plot





stopCluster(cl) # stop the cluster when done

# Scratch ----

# Comparison of sequential and parallel: identical predictions and diagnostics. 
# Accuracy: sum of true positives and true negatives, as a portion of total sample.
# Precision: True positives as a portion of all predictions
# Recall: true postives as a portion of all observed positives
# False positive rate: False positives a portion of all observed negatives

# rf.p.pred
# Observed as columns, predicted as rows.
#       0     1
# 0 67740   375
# 1  2846   694

# accuracy            0.9550
# precision           0.1960
# recall              0.6492
# false.positive.rate 0.0403



 
# # <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# # Random forest sequential ----
# # CPU intensive, but not RAM intensive. doMC should help.
# showobjsize() # from wazefunctions.R
# 
# 
# 
# system.time(rf.w <- randomForest(wazeformula,
#                                  data = fitdat.w[trainrows,],
#                                  ntree = ntree.use,
#                                  nodesize = 5,
#                                  mtry = 9)
# )
# 
# # predict on test values
# system.time(rf.w.pred <- predict(rf.w, fitdat.w[testrows, fitvars.w]))
# 
# (predtab <- table(fitdat.w$MatchEDT_buffer[testrows], rf.w.pred))
# bin.mod.diagnostics(predtab)
# 
# # rf.w.pred
# #       0     1
# # 0 67746   369
# # 1  2850   690
# # accuracy            0.9551
# # precision           0.1949
# # recall              0.6516
# # false.positive.rate 0.0404

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Conditional random forest from ctree ----

# cf.w = partykit::cforest(wazeformula,
#              data = fitdat.w[1:1000,],
#              trace = T,
#              ntree = 250)

# cat(Sys.time() - starttime, " elapsed for Waze")
# ~ 5 min for accident type only using party

format(object.size(cf.w), units = "Mb")

# Save forest output ----
outputdir_temp <- tempdir()
outputdir_final <- wazedir


# predictions
cf.w.pred <- predict(cf.w, OOB = T) # 144 Gb vector needed for predictions,


wt <- table(as.factor(fitdat.w$nMatchEDT_buffer[1:1000]), cf.w.pred)

knitr::kable(wt)

bin.mod.diagnostics(wt) 

# With party:
#       cf.w.pred
#       FALSE TRUE
# FALSE  8170 1008
# TRUE   4358 1603

# With partykit: same overall misclassification rate, but more predicted positives overall
# |      | FALSE| TRUE|
#   |:-----|-----:|----:|
#   |FALSE |  7876| 1302|
#   |TRUE  |  4075| 1886|

save(list = c('cf.w', 'cf', 'cf.pred', 'cf.w.pred'), 
     file = file.path(outputdir_temp, 
                      paste0("Random_Forest_output_", Sys.Date(), ".RData")
     )
)

filelist <- dir(outputdir_temp)[grep("RData$", dir(outputdir_temp))]
movefiles(filelist, outputdir_temp, outputdir_final)



# Clear unused objects; only named objects retained
rm(list = ls()[is.na(match(ls(), 
                           c("fitdat.w", "fitdat", "wazeformula", "cf", "cf.pred",
                             "wazedir", "wazefigdir", "codeloc")))])
gc() # Strong remval of anything except for these objects; use with caution. gc() clears unused memory.

source(file.path(codeloc, 'utility/wazefunctions.R')) # read functions back in
