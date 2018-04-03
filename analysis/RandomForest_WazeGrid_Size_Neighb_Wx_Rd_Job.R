# Random Forest work for Waze
# Set up for ATA

# Goals: produce random forest analysis for features of Waze accidents and other event types that predict matching with EDT reports. Test effects for two spatial scales (1 and 4 mile aggregations), with and without information from neighboring cells for Waze accidents and jams.

# For 1 mi grid cells, test effect of adding precipitation data (NEXRAD), jobs data from LODES, and road type data from HPMS.

# For each model, do the following tests:
# 1. April, 70/30 split
# 2. April + May, 70 / 30 split
# 3. April + May, predict June.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(aws.s3)
library(tidyverse)

# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal", "foreach", "doParallel"), dep = T)

codeloc <- "~/SDI_Waze" 

# Set grid size:
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

aws.signature::use_credentials()

# Read in data ----
# Grab any necessary data from the S3 bucket. This transfers all contents of MD_hexagons_shapefiles to the local instance. For this script, we will use shapefiles_funClass.zip (road classification data), shapefiles_rac.zip (Residence Area Characteristics), shapefiles_wac.zip (Workplace Area Characteristics). Unzip these into the local dir
s3transfer = paste("aws s3 cp s3://ata-waze/MD_hexagon_shapefiles", localdir, "--recursive --include '*'")
system(s3transfer)

for(dd in c("shapefiles_funClass.zip", "shapefiles_rac.zip", "shapefile_wac.zip")){
  uz <- paste("unzip", file.path(localdir, dd))
  system(uz)
}
# move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")

# rename data files by month. For each month, prep time and response variables
# See prep.hex() in wazefunctions.R for details.
for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData")), month = mo)
}

# Exploration of response variable: For April, only 1,600 out of 310,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater. 15,000 have 1.
# summary(w.05$nMatchEDT_buffer > 1)
# table(w.05$nMatchEDT_buffer[w.05$nMatchEDT_buffer > 1])

# Set up for parallel anaysis, 
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

(avail.cores <- parallel::detectCores()) # 4 on local, 8 on r4 instance
ntree.use = avail.cores * 50

# Analysis ----

# Models 01, 02, 03: 1 mile, no additional data, no neighbors.

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well. Here also omitting precipitation and neighboring grid cells
# Omit as predictors in this vector:
omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          "wx",
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

# Change response to nMatch for regression; output will be continuous, much more RAM intensive.
wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                            "MatchEDT_buffer_Acc"))], 
                           response = "MatchEDT_buffer_Acc")

keyoutputs = list() # to store model diagnostics

# Model 01: April ----
modelno = "01"

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]
 
system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                .combine = randomForest::combine, .multicombine=TRUE, .packages = 'randomForest') %dopar% 
          randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree) )

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
               summary(w.04$MatchEDT_buffer),
               length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                     predtab,
                     diag = bin.mod.diagnostics(predtab)
                     )

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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
              "rf.04.pred",
              "testrows",
              "trainrows",
              "w.04",
              "out.04"),
     object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
     bucket = waze.bucket)

# Model 02: April + May ----
modelno = "02"
w.0405 <- rbind(w.04, w.05)
trainrows <- sort(sample(1:nrow(w.0405), size = nrow(w.0405)*.7, replace = F))
testrows <- (1:nrow(w.0405))[!1:nrow(w.0405) %in% trainrows]

system.time(rf.0405 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.0405[trainrows,], ntree = ntree)})

rf.0405.pred <- predict(rf.0405, w.0405[testrows, fitvars])
Nobs <- data.frame(t(c(nrow(w.0405),
                       summary(w.0405$MatchEDT_buffer),
                       length(w.0405$nWazeAccident[w.0405$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.0405$MatchEDT_buffer[testrows], rf.0405.pred))
keyoutputs[[modelno]] = list(Nobs,
                          predtab,
                          diag = bin.mod.diagnostics(predtab))
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
          file = paste(modelno, "RandomForest_pred_0405.csv"),
          row.names = F)
s3save(list = c("rf.0405",
              "rf.0405.pred",
              "testrows",
              "trainrows",
              "w.0405",
              "out.0405"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405.RData", sep="_")),
       bucket = waze.bucket)

# Model 03: April + May, predict June ----
modelno = "03"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                               .combine = randomForest::combine, .multicombine = TRUE, 
                               .packages = "randomForest") %dopar%
              randomForest(wazeAccformula, data = w.0405, ntree = ntree))

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])
w.040506 <- rbind(w.0405, w.06)
Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                          predtab,
                          diag = bin.mod.diagnostics(predtab)
)

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                      TN = out.06$Obs == 0 &  out.06$Pred == 0,
                      FP = out.06$Obs == 0 &  out.06$Pred == 1,
                      FN = out.06$Obs == 1 &  out.06$Pred == 0,
                      TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
              "rf.0405.06.pred",
              "w.06",
              "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(rf.04, rf.0405.all, rf.0405); stopCluster(cl); gc()

# Model 04: April, 4 mi ----
# note file names is WazeTimeEdtHexAll, as weather data not prepped yet for 4 mi hexagons
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

HEXSIZE = "4"
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexAll_", mo,"_4mi.RData")), month = mo)
}
w.0405 <- rbind(w.04, w.05)
w.040506 <- rbind(w.0405, w.06)

modelno = "04"

# Formula for 4 mi hex
omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          "wx",
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T), # neighboring jams
          grep("^med", names(w.04), value = T), # not present in 1 mile version
          grep("nMagVar", names(w.04), value = T)
          )

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                               "MatchEDT_buffer_Acc"))], 
                              response = "MatchEDT_buffer_Acc")

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree)})

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))
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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)

# Model 05: April + May, predict June, 4 mi ----
modelno = "05"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                                   .combine = randomForest::combine, .multicombine = TRUE, 
                                   .packages = "randomForest") %dopar%
              randomForest(wazeAccformula, data = w.0405, ntree = ntree))

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])
Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
                "rf.0405.06.pred",
                "w.06",
                "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(rf.04, rf.0405.all); stopCluster(cl); gc()

# Model 08: April, 1 mi, neighbors ----
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

HEXSIZE = "1"
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData")), month = mo)
}
w.040506 <- rbind(w.04, w.05, w.06)

modelno = "08"

omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          "wx",
          grep("^med", names(w.04), value = T), # not present in 1 mile version
          grep("nMagVar", names(w.04), value = T)
)

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                               "MatchEDT_buffer_Acc"))], 
                              response = "MatchEDT_buffer_Acc")

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar%
              randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree))

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab)
)

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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)


# Model 09: April + May, predict June, 1 mi, neighbors ----
modelno = "09"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                                   .combine = randomForest::combine, .multicombine = TRUE, 
                                   .packages = "randomForest") %dopar%
              randomForest(wazeAccformula, data = w.0405, ntree = ntree))

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])

Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
                "rf.0405.06.pred",
                "w.06",
                "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(rf.04, rf.0405.all); stopCluster(cl); gc()



# Model 10: April, 4 mi, neighbors ----
# note file names is WazeTimeEdtHexAll, as weather data not prepped yet for 4 mi hexagons
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

HEXSIZE = "4"
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexAll_", mo,"_4mi.RData")), month = mo)
}
w.040506 <- rbind(w.04, w.05, w.06)

modelno = "10"

# Formula for 4 mi hex
omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          "wx",
          grep("^med", names(w.04), value = T), # not present in 1 mile version
          grep("nMagVar", names(w.04), value = T)
          )

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

# Change response to nMatch for regression; output will be continuous, much more RAM intensive.
wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                               "MatchEDT_buffer_Acc"))], 
                              response = "MatchEDT_buffer_Acc")

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree)}) 

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))

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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)

# Model 11: April + May, predict June, 4 mi, neighbors ----
modelno = "11"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                                   .combine = randomForest::combine, .multicombine = TRUE, 
                                   .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.0405, ntree = ntree) } )

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])

Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab)
)

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
                "rf.0405.06.pred",
                "w.06",
                "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(rf.04, rf.0405.06); stopCluster(cl); gc()

# Model 12: April, 1 mi, neighbors, wx ----
cl <- makeCluster(parallel::detectCores()) 
registerDoParallel(cl)

HEXSIZE = "1"
inputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Rdata_Input")
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

for(mo in c("04","05","06")){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData")), month = mo)
}
w.040506 <- rbind(w.04, w.05, w.06)

modelno = "12"

omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          grep("^med", names(w.04), value = T), # not present in 1 mile version
          grep("nMagVar", names(w.04), value = T)
          )

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                               "MatchEDT_buffer_Acc"))], 
                              response = "MatchEDT_buffer_Acc")

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree)})

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab)
)

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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)

# Model 13: April + May, predict June, 1 mi, neighbors, wx ----
modelno = "13"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                                   .combine = randomForest::combine, .multicombine = TRUE, 
                                   .packages = "randomForest") %dopar% {
              randomForest(wazeAccformula, data = w.0405, ntree = ntree)})

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])
Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
                "rf.0405.06.pred",
                "w.06",
                "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))

# cleanup
rm(rf.04, rf.0405.06); stopCluster(cl); gc()

# Model 14: April, 1 mi, neighbors, wx, roads ----
# Models 14 and 15, adding road functional class
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

modelno = "14"

for(w in c("w.04", "w.05", "w.06")){
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum")
}
w.040506 <- rbind(w.04, w.05, w.06)

omits = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.04), value = T),
          grep("^med", names(w.04), value = T), # not present in 1 mile version
          grep("nMagVar", names(w.04), value = T)
)

fitvars <- names(w.04)[is.na(match(names(w.04), omits))]

wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                               "MatchEDT_buffer_Acc"))], 
                              response = "MatchEDT_buffer_Acc")

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]

system.time(rf.04 <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                             .combine = randomForest::combine, .multicombine = TRUE, 
                             .packages = "randomForest") %dopar% {
                               randomForest(wazeAccformula, data = w.04[trainrows,], ntree = ntree)})

rf.04.pred <- predict(rf.04, w.04[testrows, fitvars])

Nobs <- data.frame(t(c(nrow(w.04),
                       summary(w.04$MatchEDT_buffer),
                       length(w.04$nWazeAccident[w.04$nWazeAccident>0]) )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.04$MatchEDT_buffer[testrows], rf.04.pred)) 

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab)
)

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
          file = paste(modelno, "RandomForest_pred_04.csv", sep = "_"),
          row.names = F)
s3save(list = c("rf.04",
                "rf.04.pred",
                "testrows",
                "trainrows",
                "w.04",
                "out.04"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_04.RData", sep= "_")),
       bucket = waze.bucket)

# Model 15: April + May, predict June, 1 mi, neighbors, wx, roads ----
modelno = "15"

system.time(rf.0405.all <- foreach(ntree = rep(ntree.use/avail.cores, avail.cores),
                                   .combine = randomForest::combine, .multicombine = TRUE, 
                                   .packages = "randomForest") %dopar% {
                                     randomForest(wazeAccformula, data = w.0405, ntree = ntree)})

rf.0405.06.pred <- predict(rf.0405.all, w.06[, fitvars])
Nobs <- data.frame(t(c(nrow(w.040506),
                       summary(w.040506$MatchEDT_buffer),
                       length(w.040506$nWazeAccident[w.040506$nWazeAccident>0]))))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
(predtab <- table(w.06$MatchEDT_buffer, rf.0405.06.pred))

keyoutputs[[modelno]] = list(Nobs,
                             predtab,
                             diag = bin.mod.diagnostics(predtab))

out.06 <- data.frame(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], rf.0405.06.pred)
names(out.06)[4:5] <- c("Obs", "Pred")
out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = paste(modelno, "RandomForest_pred_0405_06.csv", sep = "_"),
          row.names = F)

s3save(list = c("rf.0405.all",
                "rf.0405.06.pred",
                "w.06",
                "out.06"),
       object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output_0405_06.RData", sep= "_")),
       bucket = waze.bucket)

save("keyoutputs", file = paste0("Outputs_up_to_", modelno))


# Models 16 and 17, adding jobs.


stopCluster(cl) # stop the cluster when done

# Summary from keyoutputs ----

Nobs.frame <- data.frame(matrix(unlist(lapply(keyoutputs, FUN = function(x) x[[1]])), nrow = length(keyoutputs)))
rownames(Nobs.frame) <- names(keyoutputs)
colnames(Nobs.frame) <- names(keyoutputs[[1]][[1]])

Diag.frame <- data.frame(matrix(unlist(lapply(keyoutputs, FUN = function(x) x[[3]])), ncol = length(keyoutputs)))
colnames(Diag.frame) <- names(keyoutputs)
rownames(Diag.frame) <- rownames(keyoutputs[[1]][[3]])



