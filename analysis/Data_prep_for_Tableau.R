# Data prep for Tableau viz

# setup ----
library(tidyverse)

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
states = c('CT', 'MD', 'UT','VA')  #
cutoff.crash = c(0.35, 0.225, 0.215, 0.225) # See Plotting_RF_diagnostics.R
# <><><><><>

filestozip = vector()

counter = 1

for(state in states){
  # Load prepared input data
  load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData')))
  #assign(paste(state, "w.allmonths", sep="_"), w.allmonths)
  #rm(w.allmonths)

  # Load Model 30 outputs, April - September 2017 ----
  # producing <state>_All_Model_30.csv
  
  modelno = "30"
  response.var = "MatchEDT_buffer_Acc"
  # Omit as predictors in this vector:
  alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", "vmt_time",
                 "uniqWazeEvents", "nWazeRowsInMatch", 
                 "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                 grep("EDT", names(w.allmonths), value = T))
  
  alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")
  
  alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")
  
  load(file.path(outputdir, paste(state, "Model", modelno, "RandomForest_Output.RData", sep= "_")))
  
  fitvars <- rownames(rf.out$importance)
  
  train.dat = w.allmonths
  test.dat = w.allmonths # run on all hours of this time period, not 30% sample
  
  cutoff = c(1-cutoff.crash[counter], cutoff.crash[counter])
  cc <- complete.cases(test.dat[,fitvars])
  test.dat <- test.dat[cc,]
  
  Fit_all.pred <- predict(rf.out, test.dat[fitvars], cutoff = cutoff)
  Fit_all.prob <- predict(rf.out, test.dat[fitvars],  type = "prob", cutoff = cutoff)
  
  predtab <- table(test.dat[,response.var], Fit_all.pred)
  
  reference.vec <- test.dat[,response.var]
  levels(reference.vec) = c("NoCrash", "Crash")
  levels(Fit_all.pred) = c("NoCrash","Crash")
  
  reference.vec <-as.factor(as.character(reference.vec))
  Fit_all.pred <-as.factor(as.character(Fit_all.pred))
  
  (predtab <- table(Fit_all.pred, reference.vec, 
                    dnn = c("Predicted","Observed"))) 
  bin.mod.diagnostics(predtab)
  
  model_auc <- pROC::auc(test.dat[,response.var], Fit_all.prob[,colnames(Fit_all.prob)=="1"])
  
  out.df <- data.frame(test.dat[, c("GRID_ID", "day", "hour", response.var)], Fit_all.pred, Fit_all.prob)
  out.df$day <- as.numeric(out.df$day)
  names(out.df)[4:7] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
  out.df = data.frame(out.df,
                      TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                      FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                      FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                      TP = out.df$Obs == 1 &  out.df$Pred == "Crash")
  
  
  head(out.df)
  
  w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)
  
  w.group$TN[w.group$TN==TRUE] = "TN"
  w.group$TP[w.group$TP==TRUE] = "TP"
  w.group$FP[w.group$FP==TRUE] = "FP"
  w.group$FN[w.group$FN==TRUE] = "FN"
  
  w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])
  
  grp  <- as.factor(w.group$group)
  
  out.df$Pred = ifelse(out.df$Pred=="Crash", 1, 0)
  out.df$DayOfWeek = lubridate::wday(strptime(paste0("2017-", formatC(out.df$day, width = 3, flag = "0")), "%Y-%j"))
  out.df$day <- as.character(out.df$day)
  out.df$Hour <- strptime(paste0("2017-", formatC(out.df$day, width = 3, flag = "0"), " ", out.df$hour), "%Y-%j %H")
  
  dd <- data.frame(out.df, Pred.grp = grp, w.allmonths[c("nMatchEDT_buffer_Acc", fitvars, alert_subtypes)]) 
  
  #### Performance by time of day plot
  
  dim(dd)
  
  d2 <- dd %>% select(-Hour) %>% 
    group_by(hour) %>%
    summarize(N = n(),
              TotalWazeAcc = sum(nWazeAccident),
              TotalObservedEDT = sum(nMatchEDT_buffer_Acc),
              TotalEstimated = sum(Pred == 1),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(80, 150)) +
    ylab("Percent of Observed EDT crashes Estimated")
  
  paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
  labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
            "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")
  
  ggplot(d2, aes(x= hour, y= Pct_Obs_Est, fill = TotalWazeAcc)) +
    geom_line(aes(y = 100, x = 0:23), lwd = 1.5, col = "darkgreen") +
    geom_bar(stat="identity")+
    coord_polar()+
    ylim(c(0, 150)) + 
    ylab("") +
    xlab("Hour of Day") + 
    scale_y_continuous(labels = "", breaks = 1) +
    scale_x_continuous(labels = labs,
                       breaks= seq(0, 23, 2)) +
    ggtitle(paste(state, "Model 30: Estimated EDT crashes / observed \n By hour of day"))
  ggsave(file = paste0(state, "_Obs_Est_rose.jpg"), device = 'jpeg', path = "~/workingdata/Figures")
  
  write.csv(d2, file.path(outputdir, paste0(state, "_Obs_Est_EDT_Model_30_by_hour.csv")), row.names = F)
  write.csv(dd, file.path(outputdir, paste0(state, "_All_Model_30.csv")), row.names = F)
  
  filestozip = c(filestozip,
                 file.path(outputdir, paste0(state, "_Obs_Est_EDT_Model_30_by_hour.csv")),
                 file.path(outputdir, paste0(state, "_All_Model_30.csv")))
  
  counter = counter + 1
} # end state loop ----  


zipname = paste0('Multi-state_Model_', modelno, '_All_Output_', Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             paste(filestozip, collapse = " ")))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

zipname = paste0('Figures_', Sys.Date(), '.zip')

# Use -j to ignore non-informative paths in zipped file
system(paste('zip -r -j', file.path('~/workingdata', zipname),
             '~/workingdata/Figures/*'))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))



# Average by day

# remake day of week
datetime <- strptime(paste("2017", dd$day, sep = "-"), format = "%Y-%j")
dd$DayOfWeek <- as.numeric(format(datetime, "%w")) # Weekday as decimal number (0–6, Sunday is 0).
dd$Month <- as.character(format(datetime, "%B"))  # Full month name

d.day <- dd %>% 
  group_by(DayOfWeek) %>%
  summarize(N = n(),
            TotalWazeAcc = sum(nWazeAccident),
            TotalObservedEDT = sum(nMatchEDT_buffer_Acc),
            TotalEstimated = sum(Pred == "Crash"),
            Obs_Est_diff = TotalObservedEDT - TotalEstimated,
            Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)

d.day

keepcol = c("GRID_ID",
            "day",
            "hour",
            "Month",
            "DayOfWeek",
            "Obs",
            "Pred",
            "Prob.Crash",
            "FN",
            "FP",
            "TN",
            "TP",
            "Pred.grp",
            "nMatchEDT_buffer_Acc",
            "nWazeAccident")

all(keepcol %in% names(dd))

write.csv(dd[keepcol], "Subset2_Model_30.csv", row.names = F)

dd$Pred_sum = 1 # to match previous Tableau prep

keepcol = c("GRID_ID",
            "day",
            "hour",
            "DayOfWeek",
            "Pred",
            "Pred_sum",
            "nMatchEDT_buffer_Acc",
            "Month")

write.csv(dd[keepcol], "Subset2_Model_30_byMONTH.csv", row.names = F)
