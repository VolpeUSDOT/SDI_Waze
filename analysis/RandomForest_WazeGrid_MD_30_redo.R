# Running full set of random forest models
# https://github.com/VolpeUSDOT/SDI_Waze/wiki/Models-to-test
# Modified for multi-state model running, currently only model 18 run for CT and UT


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
HEXSIZE = 1 #c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

# <><><><><>
state = 'MD' #'CT'  #'UT' # Sets the state. UT, VA, MD will all be options.
# <><><><><>

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

# Check to see if this state/month combination has already been prepared, if not do the prep steps

if(length(grep(paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData'), dir(localdir)))==0){
  # Data prep ----
  
  # rename data files by month. For each month, prep time and response variables
  # See prep.hex() in wazefunctions.R for details.
  for(mo in do.months){
    prep.hex(paste0("WazeTimeEdtHexAll_", mo, "_", HEXSIZE, "mi_", state,".RData"), state = state, month = mo)
  }
  
  # Add FARS, AADT, HPMS, jobs
  na.action = "fill0"
  
  monthfiles = paste("w", do.months, sep=".")
  monthfiles = sub("-", "_", monthfiles)
   
  for(w in monthfiles){
    append.hex2(hexname = w, data.to.add = paste0("FARS_", state, "_2012_2016_sum_annual"), state = state, na.action = na.action)
    append.hex2(hexname = w, data.to.add = paste0(state, "_max_aadt_by_grid_fc_urban_vmt_factored"), state = state, na.action = na.action)
    append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_wac_sum"), state = state, na.action = na.action)
    append.hex2(hexname = w, data.to.add = paste0(state, "_hexagons_1mi_bg_rac_sum"), state = state, na.action = na.action)   
    }
  
  # Bind all months together
  
  w.allmonths <- vector()
  for(i in monthfiles){
    w.allmonths <- rbind(w.allmonths, get(i))
  }
  # end data prep
  } else {
    load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData')))
}
  

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.allmonths), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

# Load previous Model 30 ----

response.var = "MatchEDT_buffer_Acc"

omits = c(alwaysomit, alert_subtypes)
          

# Read model from local (pull down from S3 if not present). rf.out is the model, class randomForest

load(file.path(outputdir, "MD_Model_30_RandomForest_Output.RData"))

train.dat = w.allmonths
test.dat = w.allmonths # run on all months of MD

cutoff = c(0.775, 0.225)
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
cc <- complete.cases(test.dat[,fitvars])
test.dat <- test.dat[cc,]

MD_mod_30_all.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
MD_mod_30_all.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)

predtab <- table(test.dat[,response.var], MD_mod_30_all.pred)

reference.vec <- test.dat[,response.var]
levels(reference.vec) = c("NoCrash", "Crash")
levels(MD_mod_30_all.pred) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
MD_mod_30_all.pred <-as.factor(as.character(MD_mod_30_all.pred))

(predtab <- table(MD_mod_30_all.pred, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)

model_auc <- pROC::auc(test.dat[,response.var], MD_mod_30_all.prob[,colnames(MD_mod_30_all.prob)=="1"])

out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], MD_mod_30_all.pred, MD_mod_30_all.prob)
out.df$day <- as.numeric(out.df$day)
names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
out.df = data.frame(out.df,
                    TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                    FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                    FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                    TP = out.df$Obs == 1 &  out.df$Pred == "Crash")



# From Data_prep_for_Tableau.R

w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)

w.group$TN[w.group$TN==TRUE] = "TN"
w.group$TP[w.group$TP==TRUE] = "TP"
w.group$FP[w.group$FP==TRUE] = "FP"
w.group$FN[w.group$FN==TRUE] = "FN"

w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])

grp  <- as.factor(w.group$group)

identical(test.dat$GRID_ID, out.df$GRID_ID)

dd <- data.frame(out.df, Pred.grp = grp, test.dat[c("nMatchEDT_buffer_Acc", fitvars, alert_subtypes)]) 

write.csv(dd, paste0(state, "_All_Model_30.csv"), row.names = F)

savelist = c("rf.out", "rf.pred", "rf.prob", "out.df", "w.allmonths") 

fn = paste(state, "Model_30_all_RandomForest_Output.RData", sep= "_")

save(list = savelist, file = file.path(outputdir, fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(outputdir, fn),
             file.path(teambucket, "MD", fn)))


# Zip with All_Model_30.csv and put in export

zipname = paste0('RandomForest_Model_30_', state, '_All_Output_', Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(outputdir, fn),
             file.path('~/workingdata', paste0(state, "_All_Model_30.csv"))))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

             
             

