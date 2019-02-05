# Data prep for Tableau viz

# Setup ---- 
rm(list=ls()) # Start fresh
library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

# codeloc <- "~/git/SDI_Waze" 
codeloc <- "~/SDI_Waze" 

source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

# localdir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/TN"
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) 
localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

outputdir <- file.path(localdir, "Random_Forest_Output")

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

grids = c("TN_01dd_fishnet", "TN_1sqmile_hexagons")

# Manually set up do.months for now
do.months = c(paste("2017", c("04","05","06","07","08","09", "10", "11", "12"), sep="-"),
              paste("2018", c("01","02","03"), sep="-"))

# do.months = paste("2018", c("01","02","03"), sep="-")
state = "TN"
cutoff.crash = c(0.1, 0.05) # 0.1 for MatchWaze_Accident, 0.08 for TN_crash as response 

# Bundle data for Tableau

zipname = paste0('TN_GridAgg_inputs', "_", Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             "TN_2017-04_to_2018-03_TN_01dd_fishnet.csv",
             "TN_2017-04_to_2018-03_TN_1sqmile_hexagons.csv"))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


for(g in grids){ # g = grids[1]
  # Grab from S3
  # system(paste("aws s3 ls", file.path(teambucket, 'export_requests/')))
  # system(paste("aws s3 ls", file.path(teambucket, 'TN', 'RandomForest_Output/')))
  
  # system(paste("aws s3 cp", 
  #              file.path(teambucket, 'export_requests', 'TN_RandomForest_Outputs_TN_1sqmile_hexagons_2018-12-18.zip'),
  #              file.path(teambucket, 'TN', 'RandomForest_Output', 'TN_RandomForest_Outputs_TN_1sqmile_hexagons_2018-12-18.zip')
  #              
  #              ))
  # i = paste0('TN_RandomForest_Outputs_', g, '_2018-12-18.zip')
  # system(paste("aws s3 cp",
  #              file.path(teambucket, 'TN', 'RandomForest_Output', i),
  #              file.path('~', 'workingdata', 'TN', 'Random_Forest_Output', g, i)))
  # 
  # if(length(grep('zip$', i))!=0) {
  #   system(paste('unzip -o', file.path('~', 'workingdata', 'TN', 'Random_Forest_Output', g, i), '-d',
  #                file.path('~', 'workingdata', 'TN', 'Random_Forest_Output', g)))
  # }
  # 
  mods = dir(file.path(localdir, "Random_Forest_Output"))[grep(paste0(g, "_RandomForest_Output.RData$"), dir(file.path(localdir, "Random_Forest_Output")))]
  
  modelnos = substr(mods, 10, 11)
  
  diagnostics = dir(file.path(localdir))[grep("^Output_to_", dir(file.path(localdir)))]
  diagnostics = diagnostics[grep(g, diagnostics)]
  
  load(file.path(localdir, sort(diagnostics, decreasing = T)[1])) 
  
  tabl <- vector()
  for(i in names(keyoutputs)){ 
    tabl <- rbind(tabl, c(100*keyoutputs[[i]]$diag, round(keyoutputs[[i]]$auc, 4)))
  }
  
  colnames(tabl)[1:5] = c("Accuracy", "Precision", "Recall", "False Positive Rate", "AUC")
  rownames(tabl) =  names(keyoutputs)
  
  write.csv(tabl, row.names = T, file = file.path(localdir, "Random_Forest_Output", "To_Export", 
                                                  paste0(sort(diagnostics, decreasing = T)[1], "_", g, ".csv")))
  
  # For each model, make a rose plot and output the estimates as .csv
  for(j in 1:length(modelnos)){ # j = 1
    load(file.path(localdir, "Random_Forest_Output", mods[j]))
    
    modelno = modelnos[j]  
    
    cat("\n", rep("<>", 10), "\n", g, modelno,"\n\n")
  
  # Load prepared input data
  load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], "_", g, '.RData')))
  
  # Load Model <modelno> outputs ----
  # producing <state>_All_Model_<modelno>.csv
  
  if(modelno %in% c("01", "02", "03") ) {
    response.var = "MatchTN_buffer_Acc"
    cutoff.crash.use = cutoff.crash[1]
  }
  if(modelno %in% c("04", "05", "06") ) {
    response.var = "TN_crash"
    cutoff.crash.use = cutoff.crash[2]
    
  }
  
  # Omit as predictors in this vector:
  alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", 
                 "uniqueWazeEvents", "nWazeRowsInMatch", 
                 "uniqueTNreports", "TN_crash", "date",
                 "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                 grep("nTN", names(w.allmonths), value = T),
                 grep("MatchTN", names(w.allmonths), value = T),
                 grep("TN_UA", names(w.allmonths), value = T))
  
  
  alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")
  
  alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")
  
  fitvars <- rownames(rf.out$importance)
  
  train.dat = w.allmonths
  test.dat = w.allmonths # run on all hours of this time period, not 30% sample
  
  cutoff = c(1-cutoff.crash.use, cutoff.crash.use)
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
  
  out.df <- data.frame(test.dat[, c("GRID_ID", 'year', "day", "hour", response.var)], Fit_all.pred, Fit_all.prob)
  out.df$day <- as.numeric(out.df$day)
  names(out.df)[5:8] <- c("Obs", "Pred", "Prob.Noncrash", "Prob.Crash")
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
  
  # out.df$Pred = ifelse(out.df$Pred=="Crash", 1, 0)
  
  out.df$DayOfWeek = lubridate::wday(strptime(paste0(out.df$year, "-", formatC(out.df$day, width = 3, flag = "0")), "%Y-%j"))
  out.df$day <- as.character(out.df$day)
  out.df$Hour <- strptime(paste0(out.df$year, "-", formatC(out.df$day, width = 3, flag = "0"), " ", out.df$hour), "%Y-%j %H")
  
  dd <- data.frame(out.df, Pred.grp = grp, w.allmonths[c("nMatchTN_buffer_Acc", "MatchTN_buffer_Acc", "TN_crash", "nTN_total", unique(c(fitvars, alert_types, alert_subtypes)))]) 

    #### Performance by time of day plot
      
    dim(dd)
    
    if(modelno %in% c("01", "02", "03") ) {
      obs_var_to_plot = "nMatchTN_buffer_Acc"
      obs_var_to_plot_bin = "MatchTN_buffer_Acc"
    }
    if(modelno %in% c("04", "05", "06") ) {
      obs_var_to_plot = "nTN_total"
      obs_var_to_plot_bin = "TN_crash"
      
    }
    
    d2 <- dd %>% dplyr::select(-Hour) %>% 
      group_by(hour) %>%
      summarize(N = n(),
                TotalWazeAcc = sum(nWazeAccident),
                TotalObserved = sum(get(obs_var_to_plot)),
                TotalEstimated = sum(Pred == 'Crash'),
                Obs_Est_diff = TotalObserved - TotalEstimated,
                Pct_Obs_Est = 100 *TotalEstimated / TotalObserved)
    
    ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(70, 120)) +
      ylab("Percent of Observed TN crashes Estimated")
    
    
    d2 <- dd %>% dplyr::select(-Hour) %>% 
      group_by(hour) %>%
      summarize(N = n(),
                TotalWazeAcc = sum(nWazeAccident),
                TotalObserved = sum(get(obs_var_to_plot_bin)==1),
                TotalEstimated = sum(Pred == 'Crash'),
                Obs_Est_diff = TotalObserved - TotalEstimated,
                Pct_Obs_Est = 100 *TotalEstimated / TotalObserved)
    
    ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(70, 120)) +
      ylab("Percent of Observed TN crashes Estimated") 
    
    paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
    labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
              "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")
    
    ggplot(d2, aes(x= hour, y= Pct_Obs_Est, fill = TotalWazeAcc)) +
      geom_line(aes(y = 100, x = 0:23), lwd = 1.5, col = "darkgreen") +
      geom_bar(stat="identity")+
      coord_polar()+
     # ylim(c(0, 150)) + 
      ylab("") +
      xlab("Hour of Day") + 
      scale_y_continuous(labels = "", breaks = 1) +
      scale_x_continuous(labels = labs,
                         breaks= seq(0, 23, 2)) +
      ggtitle(paste("TN Model", modelno, g, ": Estimated TN crashes / observed \n By hour of day"))
    
    ggsave(file = paste0("TN_Obs_Est_", modelno, "_", g, "_rose.jpg"), device = 'jpeg', 
           path = "~/workingdata/TN/Figures", 
           width = 7, height = 7, units = 'in')
    
    # Export as csv for Tableau
    
    write.csv(dd, file.path(localdir, "Random_Forest_Output", "To_Export", paste0("TN_Model_", modelno, "_", g, ".csv")), row.names = F)
  } # end model loop
} # end grid loop



# Bundle outputs for Tableau and figures

zipname = paste0('TN_Tableau_Out', "_", Sys.Date(), '.zip')

outfiles <- figfiles <- vector()

for(g in grids){ 
  for(modelno in formatC(1:6, width = 2, flag = 0)){ # modelno = '01'
    outfiles <- c(outfiles, file.path(localdir, "Random_Forest_Output", "To_Export", paste0("TN_Model_", modelno, "_", g, ".csv")))
    figfiles <- c(figfiles, file.path(localdir, "Figures", paste0("TN_Obs_Est_", modelno, "_", g, "_rose.jpg")))
  }
}

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "Random_Forest_Output", "To_Export", 'Output_to_06_TN_01dd_fishnet_TN_01dd_fishnet.csv'),
             file.path(localdir, "Random_Forest_Output", "To_Export", 'Output_to_06_TN_1sqmile_hexagons_TN_1sqmile_hexagons.csv'),
              paste(outfiles, collapse = " "),
              paste(figfiles, collapse = " "))
             )

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

