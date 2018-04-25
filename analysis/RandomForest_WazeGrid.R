# Random Forest work for Waze

# Goals: produce random forest analysis for features of Waze accidents and other event types that predict matching with EDT reports 

# Running on gridded data, with Waze predictors only. Try three versions. For each one, produce confusion matrix, binary model diagnostics, and output .csv of grid IDs and four columns, one each for true negative, false postitive, false negative, true positive.

# This script has been re-purposed to test on local what we are running as do.rf() calls from RandomForest_WazeGrid_Fx.R  

# 1. April, 70/30 split
# 2. April + May, 70 / 30 split
# 3. April + May, predict June.

# After this is complete, repeat these three with Waze accidents and nMatchEDT_acc as response only
# If only Waze accident report
# 1. Predict to nMatchEDT_buffer_Acc (binary version, MatchEDT_buffer_Acc)
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
library(data.table)
library(ranger) 
library(pROC)
library(AUC)
# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal", "foreach", "doParallel", "ranger"), dep = T)

setwd("~/")

#Sudderth drive
#if(length(grep("EASdocs", getwd())) > 0) {mappeddrive = "S:"} 
mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load
codeloc <- "~/GitHub/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

#Flynn drive
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} 
codeloc <- "~/git/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT"))
#wazedir <- "~/Temp Working Docs/SDI_temp" # Dan local
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")

setwd(wazedir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Read in data, renaming data files by month. For each month, prep time and response variables

# Old version:
# load("Archives/WazeEDT Agg1mile Rdata Input/WazeTimeEdtHex_04.RData")

# New version of aggregation:
load("WazeEDT Agg1mile Rdata Input/WazeTimeEdtHexAll_04_1mi.RData")

class(wazeTime.edt.hexAll) <- "data.frame"

wazeTime.edt.hex <- wazeTime.edt.hexAll

wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$weekday)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)

# Going to binary for all Waze buffer match:
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)

# Going to binary for all Waze Accident buffer match:
wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.04 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeEDT Agg1mile Rdata Input/WazeTimeEdtHexAll_05_1mi.RData")
class(wazeTime.edt.hexAll) <- "data.frame"
wazeTime.edt.hex <- wazeTime.edt.hexAll

wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$weekday)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)
wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.05 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeEDT Agg1mile Rdata Input/WazeTimeEdtHexAll_06_1mi.RData")
class(wazeTime.edt.hexAll) <- "data.frame"
wazeTime.edt.hex <- wazeTime.edt.hexAll

wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$weekday)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)
wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.06 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

# Exploration of response variable: For April, only 1,600 out of 310,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater. 15,000 have 1.
# summary(w.05$nMatchEDT_buffer > 1)
# table(w.05$nMatchEDT_buffer[w.05$nMatchEDT_buffer > 1])


# Analysis ----

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well
#All Waze matches
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

# Change response to nMatch for regression; output will be continuous, much more RAM intensive.


# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Random forest parallel, previous manual approach (before function) ----
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# On small job, user time faster but system time greater. On a large job, almost exactly ncore X faster
(avail.cores <- parallel::detectCores()) # 4 on local

rf.inputs = list(ntree.use = avail.cores * 100, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

test.split = 0.3
response.var = "MatchEDT_buffer_Acc"

# Model 1: April, 70/30 ----
# Using this to test AUC calculations 
# This has been updated to match the arguments using in the function do.rf(). Previously was not setting mtry, maxnodes, or nodesize

modelno = "01"

omits = c(alwaysomit,
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]


train.dat = w.04

trainrows <- sort(sample(1:nrow(train.dat), size = nrow(train.dat)*(1-test.split), replace = F))
testrows <- (1:nrow(train.dat))[!1:nrow(train.dat) %in% trainrows]

#  length(trainrows)+length(testrows) == nrow(train.dat)

rundat = train.dat[trainrows,]
test.dat.use = train.dat[testrows,]

system.time(rf.04 <- foreach(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores,
                .combine = randomForest::combine, .multicombine = T,
                .packages = "randomForest") %dopar%
          randomForest(x = rundat[,fitvars], y = rundat[,response.var],
               maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
               mtry = rf.inputs$mtry, keep.forest = T)
)

summary(rf.04$votes)
rf.04$call
rf.04$forest$cutoff
summary(rf.04$forest)

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])
rf.04.prob <- predict(rf.04, w.04[testrows, fitvars], type = "prob")

Nobs <- data.frame(t(c(nrow(w.04),
               summary(w.04$MatchEDT_buffer_Acc),
               length(w.04$nWazeAccident[w.04$nWazeAccident>0]) 
               )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.04$MatchEDT_buffer_Acc[testrows], rf.04.pred)) 
bin.mod.diagnostics(predtab)

# save output predictions
out.04 <- data.frame(w.04[testrows,c("GRID_ID","day","hour", "MatchEDT_buffer_Acc")], rf.04.pred, rf.04.prob)
names(out.04)[4:7] <- c("Obs", "Pred", "Prob.NoCrash", "Prob.Crash")

plot(density(out.04[,"Prob.NoCrash"]), col = "red")
lines(density(out.04[,"Prob.Crash"]), col = "blue")

#using pROC
plot(pROC::roc(out.04$Obs, out.04$Prob.Crash, auc = TRUE))

(model_auc <- pROC::auc(out.04$Obs, out.04$Prob.Crash))

plot(pROC::roc(out.04$Obs, out.04$Prob.Crash, auc = TRUE))

# From Fx
(model_auc <- pROC::auc(test.dat.use[,response.var], rf.04.prob[,colnames(rf.04.prob)=="1"]))

identical(out.04$Obs, test.dat.use[,response.var])
head(data.frame(out.04$Obs, test.dat.use[,response.var]))
class(out.04$Obs)
class(test.dat.use[,response.var])

identical(w.04[testrows, "MatchEDT_buffer_Acc"], test.dat.use[,response.var])

# Other specification from AUC package
plot(model_auc2 <- AUC::roc(rf.04.prob[,colnames(rf.04.prob)=="1"], test.dat.use[,response.var]))
auc_sens <- sensitivity(rf.04.prob[,colnames(rf.04.prob)=="1"], test.dat.use[,response.var])
summary(auc_sens$cutoffs)
summary(auc_sens$measure)
auc_spec <- specificity(rf.04.prob[,colnames(rf.04.prob)=="1"], test.dat.use[,response.var])

summary(auc_spec$cutoffs)
summary(auc_spec$measure)

boxplot(rf.04.prob[,colnames(rf.04.prob)=="1"]~test.dat.use[,response.var])

rf.04.pred2 <- predict(rf.04, w.04[testrows, fitvars], type = "response", cutoff = c(0.9, 0.1))
rf.04.prob2 <- predict(rf.04, w.04[testrows, fitvars], type = "prob", cutoff = c(0.8, 0.2))

plot(pROC::roc(out.04$Obs, rf.04.prob2[,colnames(rf.04.prob2)=="1"], auc = TRUE))

reference.vec <- w.04$MatchEDT_buffer_Acc[testrows]
levels(reference.vec) = c("NoCrash", "Crash")
levels(rf.04.pred2) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
rf.04.pred2 <-as.factor(as.character(rf.04.pred2))

(predtab <- table(rf.04.pred2, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

# Choosing cutoffs. Low value is most greedy for non-crashes, high value is more greedy for crashes
co = seq(0.1, 0.9, by = 0.1)
pt.vec <-vector()
for(i in co){
  predx <- predict(rf.04, w.04[testrows, fitvars], type = "response", cutoff = c(1-i, i))
  levels(predx) = c("NoCrash","Crash")
  predx <-as.factor(as.character(predx))
  
  predtab <- table(predx, reference.vec) # Row 1: observed 
  pt.vec <- cbind(pt.vec, bin.mod.diagnostics(predtab))
  cat(i, ". ")
  }
colnames(pt.vec) = co

matplot(t(pt.vec), type = "b",
        pch = c("A", "P", "R", "F"),
        ylab = "Value",
        xlab = "Cutoff for crash classification",
        xaxt = "n")
axis(1, at = 1:length(co), labels = co)
legend("bottomleft",
       legend = c("A: Accuracy",
                  "R: Recall",
                  "P: Precision",
                  "F: False Positive Rate"),
       inset = 0.1)
title(main = "Waze-EDT crash classification, April 2017 Maryland")
# High precision: minimize false positives. Achieved with the strictest requirement for classifying as a crash
# High recall (sensitivity): minimize false negatives. Acheived with the least strict requrirement for classifiying as a crash
# Recommended threshold: 0.2 for crash (0.8 for non-crash)

# Repeat Model 1 with function ----
train.dat = w.04
response.var = "MatchEDT_buffer_Acc"
model.no = modelno
test.split = .30
class(train.dat) <- "data.frame"
rf.inputs = list(ntree.use = avail.cores * 100, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
  
mtry.use = rf.inputs$mtry


  # 70:30 split or Separate training and test data
  # use identical trainrows and testrows as above

    # trainrows <- sort(sample(1:nrow(train.dat), size = nrow(train.dat)*(1-test.split), replace = F))
    # testrows <- (1:nrow(train.dat))[!1:nrow(train.dat) %in% trainrows]
    # 
    rundat = train.dat[trainrows,]
    test.dat.use = train.dat[testrows,]

  # Start RF in parallel
  starttime = Sys.time()
  
  # make a cluster of all available cores
  cl <- makeCluster(parallel::detectCores()) 
  registerDoParallel(cl)
  
  rf.out <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores),
                    .combine = randomForest::combine, .multicombine=T, .packages = 'randomForest') %dopar% 
    randomForest(x = rundat[,fitvars], y = rundat[,response.var], 
                 ntree = ntree, mtry = mtry.use, 
                 maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
                 keep.forest = T)
  
  stopCluster(cl); rm(cl); gc(verbose = F) # Stop the cluster immediately after finished the RF
  
  timediff = Sys.time() - starttime
  cat(round(timediff,2), attr(timediff, "unit"), "to fit model", model.no, "\n")
  # End RF in parallel
  
  rf.pred <- predict(rf.out, test.dat.use[,fitvars])
  rf.prob <- predict(rf.out, test.dat.use[,fitvars], type = "prob")
  
  Nobs <- data.frame(nrow(rundat),
                     sum(as.numeric(as.character(rundat[,response.var])) == 0),
                     sum(as.numeric(as.character(rundat[,response.var])) > 0),
                     length(rundat$nWazeAccident[train.dat$nWazeAccident>0]) )
  
  colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
  predtab <- table(test.dat.use[,response.var], rf.pred)
  
  # pROC::roc - response, predictor
  model_auc <- pROC::auc(train.dat[testrows,response.var], rf.prob[,colnames(rf.prob)=="1"])
  
  model_auc <- pROC::auc(test.dat.use[,response.var], rf.prob[,colnames(rf.prob)=="1"])
  
  plot(pROC::roc(test.dat.use[,response.var], rf.prob[,colnames(rf.prob)=="1"]),
       main = paste0("Model ", model.no),
       grid=c(0.1, 0.2))
  legend("bottomright", legend = round(model_auc, 4), title = "AUC", inset = 0.1)
  
  dev.print(device = jpeg, file = paste0("AUC_", model.no, ".jpg"), width = 500, height = 500)
  
  # AUC::roc - predictions, labels
  # plot(model_auc2 <- AUC::roc(rf.out$votes[,"1"], factor(1*(rf.out$y==1))),
  #      main = paste0("Model ", model.no))
  # plot(AUC::roc(rf.out$votes[,"0"], factor(1*(rf.out$y==0))),
  #      add = T, col = "red")
  # AUC::auc(model_auc2)

  out.df <- data.frame(test.dat.use[, c("GRID_ID", "day", "hour", response.var)], rf.pred, rf.04.prob)
  out.df$day <- as.numeric(out.df$day)
  names(out.df)[4:7] <- c("Obs", "Pred", "Prob.NoCrash", "Prob.Crash")
  out.df = data.frame(out.df,
                      TN = out.df$Obs == 0 &  out.df$Pred == 0,
                      FP = out.df$Obs == 0 &  out.df$Pred == 1,
                      FN = out.df$Obs == 1 &  out.df$Pred == 0,
                      TP = out.df$Obs == 1 &  out.df$Pred == 1)

  # plotting probabilities. Nearly all values 
  plot(density(out.df[,"Prob.NoCrash"]), col = "red")
  lines(density(out.df[,"Prob.Crash"]), col = "blue")
  
  #using pROC as before
  plot(pROC::roc(out.df$Obs, out.df$Prob.Crash, auc = TRUE))
  
  (model_auc <- pROC::auc(out.df$Obs, out.df$Prob.Crash))
  
  # Output is list of three elements: Nobs data frame, predtab table, binary model diagnotics table, and mean squared error
  list(Nobs, predtab, diag = bin.mod.diagnostics(predtab), 
       mse = mean(as.numeric(as.character(test.dat.use[,response.var])) - 
                    as.numeric(as.character(rf.pred)))^2,
       runtime = timediff
       , auc = model_auc
  ) 


varImpPlot(rf.04) # variable imporatance: mean decrease in Gini impurity for this predictor across all trees.  

varImpPlot(rf.out) # variable imporatance: mean decrease in Gini impurity for this predictor across all trees.  


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

# Model 3: April + May, predict June ----
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


# For Waze accidents only ----
# To do: make all of this a function for faster iteration

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

(avail.cores <- parallel::detectCores()) # 4 on local
ntree.use = avail.cores * 200 # can do more trees, much smaller data set  

wazeformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                            "MatchEDT_buffer_Acc"))], 
                           response = "MatchEDT_buffer_Acc")

# Model 4-Acc: April, 70/30 ----

w.04.acc <- w.04[w.04$nWazeAccident > 0,]# ; nrow(w.04.acc) / nrow(w.04) # 5.5% of all data

trainrows <- sort(sample(1:nrow(w.04.acc), size = nrow(w.04.acc)*.7, replace = F))
testrows <- (1:nrow(w.04.acc))[!1:nrow(w.04.acc) %in% trainrows]

rf.04.acc <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                               .combine = combine,
                               .packages = "randomForest") %dopar%
              randomForest(wazeformula,
                           data = w.04.acc[trainrows,],
                           ntree = ntree)


rf.04.acc.pred <- predict(rf.04.acc, w.04.acc[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04.acc),
                       summary(w.04.acc$MatchEDT_buffer_Acc),
                       length(w.04.acc$nWazeAccident[w.04.acc$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.04.acc$MatchEDT_buffer_Acc[testrows], rf.04.acc.pred))
bin.mod.diagnostics(predtab)

# save output predictions

out.04.acc <- data.frame(w.04.acc[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer_Acc")], rf.04.acc.pred)
out.04.acc$day <- as.numeric(out.04.acc$day)

names(out.04.acc)[4:5] <- c("Obs", "Pred")

out.04.acc = data.frame(out.04.acc,
                      TN = out.04.acc$Obs == 0 &  out.04.acc$Pred == 0,
                      FP = out.04.acc$Obs == 0 &  out.04.acc$Pred == 1,
                      FN = out.04.acc$Obs == 1 &  out.04.acc$Pred == 0,
                      TP = out.04.acc$Obs == 1 &  out.04.acc$Pred == 1)
write.csv(out.04.acc,
          file = "RandomForest_pred_04.acc.csv",
          row.names = F)

save(list = c("rf.04.acc",
              "rf.04.acc.pred",
              "testrows",
              "trainrows",
              "w.04.acc",
              "out.04.acc"),
     file = "RandomForest_Output_04.acc.RData")

varImpPlot(rf.04.acc) # variable importance plot

# Model 5-Acc: April+May, 70/30 ----

w.05.acc <- w.05[w.05$nWazeAccident > 0,]
w.0405.acc <- rbind(w.04.acc, w.05.acc)

trainrows <- sort(sample(1:nrow(w.0405.acc), size = nrow(w.0405.acc)*.7, replace = F))
testrows <- (1:nrow(w.0405.acc))[!1:nrow(w.0405.acc) %in% trainrows]

rf.0405.acc <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                     .combine = combine,
                     .packages = "randomForest") %dopar%
  randomForest(wazeformula,
               data = w.0405.acc[trainrows,],
               ntree = ntree)


rf.0405.acc.pred <- predict(rf.0405.acc, w.0405.acc[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.0405.acc),
                       summary(w.0405.acc$MatchEDT_buffer_Acc),
                       length(w.0405.acc$nWazeAccident[w.0405.acc$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.0405.acc$MatchEDT_buffer_Acc[testrows], rf.0405.acc.pred))
bin.mod.diagnostics(predtab)

# save output predictions

out.0405.acc <- data.frame(w.0405.acc[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], rf.0405.acc.pred)
out.0405.acc$day <- as.numeric(out.0405.acc$day)

names(out.0405.acc)[4:5] <- c("Obs", "Pred")

out.0405.acc = data.frame(out.0405.acc,
                        TN = out.0405.acc$Obs == 0 &  out.0405.acc$Pred == 0,
                        FP = out.0405.acc$Obs == 0 &  out.0405.acc$Pred == 1,
                        FN = out.0405.acc$Obs == 1 &  out.0405.acc$Pred == 0,
                        TP = out.0405.acc$Obs == 1 &  out.0405.acc$Pred == 1)
write.csv(out.0405.acc,
          file = "RandomForest_pred_0405.acc.csv",
          row.names = F)

save(list = c("rf.0405.acc",
              "rf.0405.acc.pred",
              "testrows",
              "trainrows",
              "w.0405.acc",
              "out.0405.acc"),
     file = "RandomForest_Output_0405.acc.RData")

varImpPlot(rf.0405.acc) # variable importance plot

# Model 6-Acc: April+May, predict June ----

w.06.acc <- w.06[w.06$nWazeAccident > 0,]

system.time(rf.0405.acc.all <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                                   .combine = combine,
                                   .packages = "randomForest") %dopar%
              randomForest(wazeformula,
                           data = w.0405.acc,
                           ntree = ntree,
                           nodesize = 5,
                           mtry = 9)
)


system.time(rf.0405.06.acc.pred <- predict(rf.0405.acc.all, w.06.acc[, fitvars]))

w.040506.acc <- rbind(w.0405.acc, w.06.acc)

Nobs <- data.frame(t(c(nrow(w.040506.acc),
                       summary(w.040506.acc$MatchEDT_buffer_Acc),
                       length(w.040506.acc$nWazeAccident[w.040506.acc$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.06.acc$MatchEDT_buffer, rf.0405.06.acc.pred))
bin.mod.diagnostics(predtab)


# save output predictions

out.06.acc <- data.frame(w.06.acc[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.acc.pred)

names(out.06.acc)[4:5] <- c("Obs", "Pred")

out.06.acc = data.frame(out.06.acc,
                    TN = out.06.acc$Obs == 0 &  out.06.acc$Pred == 0,
                    FP = out.06.acc$Obs == 0 &  out.06.acc$Pred == 1,
                    FN = out.06.acc$Obs == 1 &  out.06.acc$Pred == 0,
                    TP = out.06.acc$Obs == 1 &  out.06.acc$Pred == 1)
write.csv(out.06.acc,
          file = "RandomForest_pred_0405_06_acc.csv",
          row.names = F)

save(list = c("rf.0405.acc.all",
              "rf.0405.06.acc.pred",
              "w.06.acc",
              "out.06.acc"),
     file = "RandomForest_Output_0405_06_Acc.RData")

varImpPlot(rf.0405.acc.all) # variable importance plot





stopCluster(cl) # stop the cluster when done


# Model 4 (Waze accident only EDT match response variable): April, 70/30 ----

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

# length(testrows) + length(trainrows) == nrow(w.04)

system.time(rf.04.AccMatch <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = T,
                             .packages = "randomForest") %dopar%
              randomForest(wazeAccformula,
                           data = w.04[trainrows,],
                           ntree = ntree)
)


system.time(rf.04.AccMatch.pred <- predict(rf.04.AccMatch, w.04[testrows, fitvars]))

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer_Acc),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) 
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.04$MatchEDT_buffer_Acc[testrows], rf.04.AccMatch.pred)) 

bin.mod.diagnostics(predtab)

# save output predictions

out.04.AccMatch <- data.frame(w.04[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer_Acc")], rf.04.AccMatch.pred)
out.04.AccMatch$day <- as.numeric(out.04.AccMatch$day)
names(out.04.AccMatch)[4:5] <- c("Obs", "Pred")

out.04.AccMatch = data.frame(out.04.AccMatch,
                    TN = out.04.AccMatch$Obs == 0 &  out.04.AccMatch$Pred == 0,
                    FP = out.04.AccMatch$Obs == 0 &  out.04.AccMatch$Pred == 1,
                    FN = out.04.AccMatch$Obs == 1 &  out.04.AccMatch$Pred == 0,
                    TP = out.04.AccMatch$Obs == 1 &  out.04.AccMatch$Pred == 1)
write.csv(out.04.AccMatch,
          file = "RandomForest_pred_04_AccMatch.csv",
          row.names = F)

varImpPlot(rf.04.AccMatch) # variable imporatance: mean decrease in Gini impurity for this predictor across all trees.  

save(list = c("rf.04.AccMatch",
              "rf.04.AccMatch.pred",
              "testrows",
              "trainrows",
              "w.04",
              "out.04.AccMatch"),
     file = "RandomForest_Output_04_AccMatch.RData")


varImpPlot(rf.04.AccMatch) # variable importance plot


##

# Model 5 (Waze accident only EDT match response variable): April + May, 70/30 ----
# ~ 7 min to run on 460k rows training data with 4 cores

w.0405 <- rbind(w.04, w.05)

trainrows <- sort(sample(1:nrow(w.0405), size = nrow(w.0405)*.7, replace = F))
testrows <- (1:nrow(w.0405))[!1:nrow(w.0405) %in% trainrows]

system.time(rf.AccMatch.0405 <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                               .combine = combine,
                               .packages = "randomForest") %dopar%
              randomForest(wazeAccformula,
                           data = w.0405[trainrows,],
                           ntree = ntree,
                           nodesize = 5,
                           mtry = 9)
)


system.time(rf.AccMatch.0405.pred <- predict(rf.AccMatch.0405, w.0405[testrows, fitvars]))

Nobs <- data.frame(t(c(nrow(w.0405),
                       summary(w.0405$MatchEDT_buffer_Acc),
                       length(w.0405$nWazeAccident[w.0405$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.0405$MatchEDT_buffer_Acc[testrows], rf.AccMatch.0405.pred))
bin.mod.diagnostics(predtab)

# save output predictions

out.0405.AccMatch <- data.frame(w.0405[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer_Acc")], rf.AccMatch.0405.pred)
out.0405.AccMatch$day <- as.numeric(out.0405.AccMatch$day)

names(out.0405.AccMatch)[4:5] <- c("Obs", "Pred")

out.0405.AccMatch = data.frame(out.0405.AccMatch,
                      TN = out.0405.AccMatch$Obs == 0 &  out.0405.AccMatch$Pred == 0,
                      FP = out.0405.AccMatch$Obs == 0 &  out.0405.AccMatch$Pred == 1,
                      FN = out.0405.AccMatch$Obs == 1 &  out.0405.AccMatch$Pred == 0,
                      TP = out.0405.AccMatch$Obs == 1 &  out.0405.AccMatch$Pred == 1)
write.csv(out.0405.AccMatch,
          file = "RandomForest_pred_0405_AccMatch.csv",
          row.names = F)

save(list = c("rf.AccMatch.0405",
              "rf.AccMatch.0405.pred",
              "testrows",
              "trainrows",
              "w.0405",
              "out.0405.AccMatch"),
     file = "RandomForest_Output_0405_AccMatch.RData")

varImpPlot(rf.AccMatch.0405) # variable importance plot

# Model 6 (Waze accident only EDT match response variable): April + May, predict June ----
# ~ X min to run on 657k rows training data on 4 cores

system.time(rf.AccMatch.0405.all <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                                   .combine = combine,
                                   .packages = "randomForest") %dopar%
              randomForest(wazeAccformula,
                           data = w.0405,
                           ntree = ntree,
                           nodesize = 5,
                           mtry = 9)
)


system.time(rf.AccMatch.0405.06.pred <- predict(rf.AccMatch.0405.all, w.06[, fitvars]))

w.040506 <- rbind(w.0405, w.06)

Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0])
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.06$MatchEDT_buffer, rf.AccMatch.0405.06.pred))
bin.mod.diagnostics(predtab)


# save output predictions

out.06.AccMatch <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.AccMatch.0405.06.pred)

names(out.06.AccMatch)[4:5] <- c("Obs", "Pred")

out.06.AccMatch = data.frame(out.06.AccMatch,
                    TN = out.06.AccMatch$Obs == 0 &  out.06.AccMatch$Pred == 0,
                    FP = out.06.AccMatch$Obs == 0 &  out.06.AccMatch$Pred == 1,
                    FN = out.06.AccMatch$Obs == 1 &  out.06.AccMatch$Pred == 0,
                    TP = out.06.AccMatch$Obs == 1 &  out.06.AccMatch$Pred == 1)
write.csv(out.06.AccMatch,
          file = "RandomForest_pred_0405_06_AccMatch.csv",
          row.names = F)

save(list = c("rf.AccMatch.0405.all",
              "rf.AccMatch.0405.06.pred",
              "w.06",
              "out.06.AccMatch"),
     file = "RandomForest_Output_0405_06_AccMatch.RData")

varImpPlot(rf.AccMatch.0405.all) # variable importance plot


stopCluster(cl) # stop the cluster when done
