# Running full set of random forest models
# https://github.com/VolpeUSDOT/SDI_Waze/wiki/Phase-2-Models-to-Test


# Setup ---- 
rm(list=ls()) # Start fresh
library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

codeloc <- "~/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

# Set grid size:
HEXSIZE = 1 

# Manually setting months to run here; could also scan S3 for months available for this state
do.months = paste("2017", c("04","05","06","07","08","09"), sep="-")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

outputdir <- file.path(localdir, "Random_Forest_Output")

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# check if already completed transfer of necessary supplemental data on this instance
if(length(dir(localdir)[grep("aadt_by_grid", dir(file.path(localdir, 'AADT')))]) == 0){

  source(file.path(codeloc, "utility/Workstation_setup.R"))
  # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
  system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
}

# View the files available in S3 for this state: system(paste0('aws s3 ls ', teambucket, '/', state, '/'))

# Loop over months to prepare for comparison

# <><><><><>
states = c('CT', 'MD')  #'UT' # Sets the state. UT, VA, MD are all options.
# <><><><><>

for(state in states){
  
  if(length(grep(paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData'), dir(localdir)))==0){
    # Data prep ----
    
  cat("Compiling input data for", state, "\n")
  # rename data files by month. For each month, prep time and response variables
  # See prep.hex() in wazefunctions.R for details.
  for(mo in do.months){
    prep.hex(paste0("WazeTimeEdtHexAll_", mo, "_", HEXSIZE, "mi_", state,".RData"), state = state, month = mo)
  }
  
    # Adding FARS, AADT, VMT, jobs
    na.action = "fill0"
    
    
    # Append supplmental data. This is now a time-intensive step, with hourly VMT; consider making this parallel
    
    for(w in monthfiles){ # w = "w.2017_04"
      append.hex2(hexname = w, data.to.add = paste0("FARS_", state, "_2012_2016_sum_annual"), state = state, na.action = na.action)
      append.hex2(hexname = w, data.to.add = paste0(state, "_max_aadt_by_grid_fc_urban_vmt_factored"), state = state, na.action = na.action)
      append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_wac_sum"), state = state, na.action = na.action)
      append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_rac_sum"), state = state, na.action = na.action)  
    }
    
    
    # Bind all months together, and remove monthly files to save memory
    
    w.allmonths <- vector()
    for(i in monthfiles){
      w.allmonths <- rbind(w.allmonths, get(i))
    }
    rm(list = monthfiles)
    
    save("w.allmonths", file = file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData')))
    # end data prep
  } else {
    load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData')))
  }
  
  assign(paste(state, "w.allmonths", sep="_"), w.allmonths)
  rm(w.allmonths)
  
} # End state data prep for comparison. Now have <state>_w.allmonths data frames for each state

stopifnot(identical(names(MD_w.allmonths), names(CT_w.allmonths)))

# format(object.size(w.allmonths), "Gb")

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage below max if on r4.4xlarge instance

# Use this to set number of decision trees to use, and key RF parameters. mtry is especially important, should consider tuning this with caret package
# For now use same parameters for all models for comparision; tune parameters after models are selected
rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(MD_w.allmonths), value = T), "day", "hextime", "year", "weekday", "vmt_time",
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(MD_w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"


# Compare MD and CT Model 18 ----

# Run MD Model 18

starttime = Sys.time()

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(MD_w.allmonths), value = T), # road class
          grep("SUM_MAX_AADT", names(MD_w.allmonths), value = T), # AADT
          grep("HOURLY_MAX_AADT", names(MD_w.allmonths), value = T), # Hourly VMT
          grep("WAC", names(MD_w.allmonths), value = T), # Jobs workplace
          grep("RAC", names(MD_w.allmonths), value = T), # Jobs residential
          grep("MagVar", names(MD_w.allmonths), value = T), # direction of travel
          grep("medLast", names(MD_w.allmonths), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(MD_w.allmonths), value = T), # neighboring accidents
          grep("nWazeJam_", names(MD_w.allmonths), value = T) # neighboring jams
)

modelno = "18"

keyoutputs[[paste("MD", modelno, sep="_")]] = do.rf(train.dat = MD_w.allmonths, 
                                omits, response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 

# Run CT Model 18

keyoutputs[[paste("CT", modelno, sep="_")]] = do.rf(train.dat = CT_w.allmonths, 
                                                    omits, response.var = "MatchEDT_buffer_Acc", 
                                                    model.no = modelno, rf.inputs = rf.inputs) 

fn = paste0("Model_18_Output_to_CT.RData")

save("keyoutputs", file = file.path(outputdir, fn))

# Run MD Model 18 on CT data ----

# Read model from local (pull down from S3 if not present). rf.out is the model, class randomForest

load(file.path(outputdir, "MD_Model_18_RandomForest_Output.RData"))

train.dat = MD_w.allmonths
test.dat = CT_w.allmonths # run on all months of CT for now

cutoff = c(0.775, 0.225)
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
cc <- complete.cases(test.dat[,fitvars])
test.dat <- test.dat[cc,]

MD_mod_CT_dat.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
MD_mod_CT_dat.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)

predtab <- table(test.dat[,response.var], MD_mod_CT_dat.pred)

reference.vec <- test.dat[,response.var]
levels(reference.vec) = c("NoCrash", "Crash")
levels(MD_mod_CT_dat.pred) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
MD_mod_CT_dat.pred <-as.factor(as.character(MD_mod_CT_dat.pred))

(predtab <- table(MD_mod_CT_dat.pred, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

model_auc <- pROC::auc(test.dat[,response.var], MD_mod_CT_dat.prob[,colnames(MD_mod_CT_dat.prob)=="1"])

out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], MD_mod_CT_dat.pred, MD_mod_CT_dat.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")

savelist = c("rf.out", "rf.pred", "rf.prob", "out.df") 

fn = paste("Model_18_MD_mod_CT_dat_RandomForest_Output.RData", sep= "_")

save(list = savelist, file = file.path(outputdir, fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "MD", fn)))

outlist =  list(predtab, diag = bin.mod.diagnostics(predtab), 
                mse = mean(as.numeric(as.character(test.dat[,response.var])) - 
                             as.numeric(MD_mod_CT_dat.prob[,"1"]))^2,
                auc = as.numeric(model_auc) 
) 

keyoutputs[["Model_18_MD_mod_CT"]] = outlist

fn = paste0("Model_18_MD_mod_CT_data_Output.RData")

save("keyoutputs", file = file.path(outputdir, fn))

# Run CT Model 18 on MD data ----

load(file.path(outputdir, "CT_Model_18_RandomForest_Output.RData"))

train.dat = CT_w.allmonths
test.dat = MD_w.allmonths # run on all months of CT for now

cutoff = c(0.775, 0.225)
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
cc <- complete.cases(test.dat[,fitvars])
test.dat <- test.dat[cc,]

CT_mod_MD_dat.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
CT_mod_MD_dat.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)

predtab <- table(test.dat[,response.var], CT_mod_MD_dat.pred)

reference.vec <- test.dat[,response.var]
levels(reference.vec) = c("NoCrash", "Crash")
levels(CT_mod_MD_dat.pred) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
CT_mod_MD_dat.pred <-as.factor(as.character(CT_mod_MD_dat.pred))

(predtab <- table(CT_mod_MD_dat.pred, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

model_auc <- pROC::auc(test.dat[,response.var], CT_mod_MD_dat.prob[,colnames(CT_mod_MD_dat.prob)=="1"])

out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], CT_mod_MD_dat.pred, CT_mod_MD_dat.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")

savelist = c("rf.out", "rf.pred", "rf.prob", "out.df") 

fn = paste("Model_18_CT_mod_MD_dat_RandomForest_Output.RData", sep= "_")

save(list = savelist, file = file.path(outputdir, fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "CT", fn)))

outlist =  list(predtab, diag = bin.mod.diagnostics(predtab), 
                mse = mean(as.numeric(as.character(test.dat[,response.var])) - 
                             as.numeric(CT_mod_MD_dat.prob[,"1"]))^2,
                auc = as.numeric(model_auc) 
) 

keyoutputs[["Model_18_CT_mod_MD_dat"]] = outlist

fn = paste0("Model_18_CT_mod_MD_data_Output.RData")

save("keyoutputs", file = file.path(outputdir, fn))

# Save outputs to S3

system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "CT", fn)))

# Model 30: MD model on CT data ----

omits = c(alwaysomit, alert_subtypes,
          #"wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.allmonths), value = T), # road class
          grep("SUM_MAX_AADT", names(MD_w.allmonths), value = T) # AADT
          #grep("HOURLY_MAX_AADT", names(w.allmonths), value = T), # Hourly VMT
          #grep("WAC", names(w.allmonths), value = T), # Jobs workplace
          #grep("RAC", names(w.allmonths), value = T), # Jobs residential
          #grep("MagVar", names(w.allmonths), value = T), # direction of travel
          #grep("medLast", names(w.allmonths), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.allmonths), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.allmonths), value = T) # neighboring jams
)


# Read model from local (pull down from S3 if not present). rf.out is the model, class randomForest

load(file.path(outputdir, "MD_Model_30_RandomForest_Output.RData"))

train.dat = MD_w.allmonths
test.dat = CT_w.allmonths # run on all months of CT for now

cutoff = c(0.775, 0.225)
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
cc <- complete.cases(test.dat[,fitvars])
test.dat <- test.dat[cc,]

MD_mod_CT_dat.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
MD_mod_CT_dat.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)

predtab <- table(test.dat[,response.var], MD_mod_CT_dat.pred)

reference.vec <- test.dat[,response.var]
levels(reference.vec) = c("NoCrash", "Crash")
levels(MD_mod_CT_dat.pred) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
MD_mod_CT_dat.pred <-as.factor(as.character(MD_mod_CT_dat.pred))

(predtab <- table(MD_mod_CT_dat.pred, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

model_auc <- pROC::auc(test.dat[,response.var], MD_mod_CT_dat.prob[,colnames(MD_mod_CT_dat.prob)=="1"])

out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], MD_mod_CT_dat.pred, MD_mod_CT_dat.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")

savelist = c("rf.out", "rf.pred", "rf.prob", "out.df") 

fn = paste("Model_30_MD_mod_CT_dat_RandomForest_Output.RData", sep= "_")

save(list = savelist, file = file.path(outputdir, fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "MD", fn)))

outlist =  list(predtab, diag = bin.mod.diagnostics(predtab), 
                mse = mean(as.numeric(as.character(test.dat[,response.var])) - 
                             as.numeric(MD_mod_CT_dat.prob[,"1"]))^2,
                auc = as.numeric(model_auc) 
) 

keyoutputs[["Model_30_MD_mod_CT"]] = outlist

fn = paste0("Model_30_MD_mod_CT_data_Output.RData")

save("keyoutputs", file = file.path(outputdir, fn))

# Run CT Model 30 on MD data ----

load(file.path(outputdir, "CT_Model_30_RandomForest_Output.RData"))

train.dat = CT_w.allmonths
test.dat = MD_w.allmonths # run on all months of CT for now

cutoff = c(0.775, 0.225)
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
cc <- complete.cases(test.dat[,fitvars])
test.dat <- test.dat[cc,]

CT_mod_MD_dat.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
CT_mod_MD_dat.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)

predtab <- table(test.dat[,response.var], CT_mod_MD_dat.pred)

reference.vec <- test.dat[,response.var]
levels(reference.vec) = c("NoCrash", "Crash")
levels(CT_mod_MD_dat.pred) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
CT_mod_MD_dat.pred <-as.factor(as.character(CT_mod_MD_dat.pred))

(predtab <- table(CT_mod_MD_dat.pred, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

model_auc <- pROC::auc(test.dat[,response.var], CT_mod_MD_dat.prob[,colnames(CT_mod_MD_dat.prob)=="1"])

out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], CT_mod_MD_dat.pred, CT_mod_MD_dat.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")

savelist = c("rf.out", "rf.pred", "rf.prob", "out.df") 

fn = paste("Model_30_CT_mod_MD_dat_RandomForest_Output.RData", sep= "_")

save(list = savelist, file = file.path(outputdir, fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "CT", fn)))

outlist =  list(predtab, diag = bin.mod.diagnostics(predtab), 
                mse = mean(as.numeric(as.character(test.dat[,response.var])) - 
                             as.numeric(CT_mod_MD_dat.prob[,"1"]))^2,
                auc = as.numeric(model_auc) 
) 

keyoutputs[["Model_30_CT_mod_MD_dat"]] = outlist

fn = paste0("Model_30_CT_mod_MD_data_Output.RData")

save("keyoutputs", file = file.path(outputdir, fn))

# Save outputs to S3

system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "CT", fn)))


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "to complete script")


# Save all to export ----

# Using system zip:
# system(paste('zip ~/workingdata/zipfilename.zip ~/path/to/your/file'))

zipname = paste0('VMT_RandomForest_Outputs_', Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(outputdir, 'MD_VMT_Output_to_30.RData'),
             file.path(outputdir, 'CT_VMT_Output_to_30.RData'),
             file.path(outputdir, 'Model_18_Output_to_CT.RData'),
             file.path(outputdir, 'Model_18_CT_mod_MD_data_Output.RData'),
             file.path(outputdir, 'Model_18_CT_mod_MD_dat_RandomForest_Output.RData'),
             file.path(outputdir, 'Model_18_MD_mod_CT_data_Output.RData'),
             file.path(outputdir, 'Model_18_MD_mod_CT_dat_RandomForest_Output.RData'),
             file.path(outputdir, 'Model_30_CT_mod_MD_data_Output.RData'),
             file.path(outputdir, 'Model_30_CT_mod_MD_dat_RandomForest_Output.RData'),
             file.path(outputdir, 'Model_30_MD_mod_CT_data_Output.RData'),
             file.path(outputdir, 'Model_30_MD_mod_CT_dat_RandomForest_Output.RData'),
             file.path(outputdir, 'MD_Model_63_RandomForest_Output.RData'),
             file.path(outputdir, 'MD_Model_62_RandomForest_Output.RData'),
             file.path(outputdir, 'MD_Model_61_RandomForest_Output.RData'),
             file.path(outputdir, 'MD_Model_30_RandomForest_Output.RData'),
             file.path(outputdir, 'CT_Model_63_RandomForest_Output.RData'),
             file.path(outputdir, 'CT_Model_62_RandomForest_Output.RData'),
             file.path(outputdir, 'CT_Model_61_RandomForest_Output.RData'),
             file.path(outputdir, 'CT_Model_30_RandomForest_Output.RData'),
             file.path(outputdir, 'CT_Model_18_RandomForest_Output.RData'),
             file.path(outputdir, 'MD_Model_18_RandomForest_Output.RData')
))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

