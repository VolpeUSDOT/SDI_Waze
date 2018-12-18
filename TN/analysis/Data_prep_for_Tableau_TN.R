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

codeloc <- "~/git/SDI_Waze" 
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

localdir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/TN"

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

grids = c("TN_01dd_fishnet", "TN_1sqmile_hexagons")

for(g in grids){ #g = grids[1]

  mods = dir(file.path(localdir, g, "Random_Forest_Output"))
  
  modelnos = substr(mods, 10, 11)
  
  diagnostics = dir(file.path(localdir, g))[grep("_Output_to_", dir(file.path(localdir, g)))]
  
  
  load(file.path(localdir, g, sort(diagnostics, decreasing = T)[1])) 
  
  tabl <- vector()
  for(i in names(keyoutputs)){ 
    tabl <- rbind(tabl, c(100*keyoutputs[[i]]$diag, round(keyoutputs[[i]]$auc, 4)))
    }
  
  colnames(tabl)[1:5] = c("Accuracy", "Precision", "Recall", "False Positive Rate", "AUC")
  rownames(tabl) =  names(keyoutputs)
  
  write.csv(tabl, row.names = T, file = paste0(diagnostics, "_", g, ".csv"))
  
  # For each model, make a rose plot and output the estimates as .csv
  for(j in 1:length(modelnos)){ # j = 1
    load(file.path(localdir, g, "Random_Forest_Output", mods[j]))
    
    modelno = modelnos[j]  
    
    #### Performance by time of day plot
      
    dim(out.df)
    
    d2 <- out.df %>% 
      group_by(hour) %>%
      summarize(N = n(),
                TotalObserved = sum(Obs == "1"),
                TotalEstimated = sum(Pred == "Crash"),
                Obs_Est_diff = TotalObserved - TotalEstimated,
                Pct_Obs_Est = 100 *TotalEstimated / TotalObserved)
    
    ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(50, 150)) +
      ylab("Percent of Observed TN crashes Estimated")
    
    paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
    labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
              "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")
    
    ggplot(d2, aes(x= hour, y= Pct_Obs_Est, fill = TotalObserved)) +
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
           #path = "~/workingdata/Figures", 
           width = 7, height = 7, units = 'in')
    
    # Export as csv for Tableau
    w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)
    
    w.group$TN[w.group$TN==TRUE] = "TN"
    w.group$TP[w.group$TP==TRUE] = "TP"
    w.group$FP[w.group$FP==TRUE] = "FP"
    w.group$FN[w.group$FN==TRUE] = "FN"
    
    w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])
    
    grp  <- as.factor(w.group$group)
    
    out.df$Pred = ifelse(out.df$Pred=="Crash", 1, 0)
    out.df$DayOfWeek = lubridate::wday(strptime(paste0("2018-", formatC(out.df$day, width = 3, flag = "0")), "%Y-%j"))
    out.df$day <- as.character(out.df$day)
    out.df$Hour <- strptime(paste0("2018-", formatC(out.df$day, width = 3, flag = "0"), " ", out.df$hour), "%Y-%j %H")
    
    out.df <- data.frame(out.df, Pred.grp = grp)
    
    write.csv(out.df, file.path(localdir, paste0("TN_Model_",modelno, "_", g, ".csv")), row.names = F)
  } # end model loop
} # end grid loop

