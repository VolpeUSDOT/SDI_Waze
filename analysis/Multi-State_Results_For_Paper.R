# Reading the results of the multi-state model runs, for full year of 2018, for CT/MD/UT/VA

# Setup ---- 
rm(list=ls()) # Start fresh

# Directories
codeloc <- "~/git/SDI_Waze" 
localdir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output"
outputdir <- file.path(localdir, "Random_Forest_Output", "Multi-state_2019", "Refit")
setwd(localdir)

# Functions
source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages
source(file.path(codeloc, 'utility/wazefunctions.R'))
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# Packages
library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

# Run arguments
HEXSIZE = 1 # grid size. All at 1 sq miles
do.months = paste("2018", formatC(1:12, flag = 0, width = 2), sep="-")
REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf
states = c('CT', 'MD', 'UT','VA')  
modelnos = c(18:25, 30:32)

counts = vector() # To store confusion matrix outputs

for(state in states){ 
  # state = 'CT'
  for(modelno in modelnos){
  # modelno = 18  
  cat("\n", rep("<>", 10), "\n", state, modelno,"\n\n")
  
  # TODO: fix file extensions, should be .csv, not RData
  out.df <- read.csv(file.path(outputdir, paste(state, "Model", modelno, "RandomForest_Output.RData", sep= "_")))
  
  reference.vec <- as.factor(out.df[,'Obs'])
  levels(reference.vec) = c("NoCrash", "Crash")

  reference.vec <- as.factor(as.character(reference.vec))
  Fit_all.pred <- as.factor(as.character(out.df[,'Pred']))
  
  (predtab <- table(Fit_all.pred, reference.vec, 
                    dnn = c("Predicted","Observed"))) 
  
  preds <- as.vector(predtab)
  names(preds) = c('TP', 'FN', 'FP', 'TN')
  
  diags <- as.vector(t(bin.mod.diagnostics(predtab)))
  names(diags) = c('accuracy', 'precision', 'recall', 'false.positive.rate')
  
  model_auc <- pROC::auc(reference.vec, out.df[,'Prob.Crash'])
  
  counts <- rbind(counts, c(state = state, model = modelno, diags, preds, AUC = as.numeric(model_auc)))
  
  out.df$DayOfWeek = lubridate::wday(strptime(paste0(out.df$Year, "-", formatC(out.df$day, width = 3, flag = "0")), "%Y-%j"))
  out.df$day <- as.character(out.df$day)
  out.df$Hour <- strptime(paste0(out.df$Year, "-", formatC(out.df$day, width = 3, flag = "0"), " ", out.df$hour), "%Y-%j %H")
  
  #### Performance by time of day plot
  
  dim(out.df)
  
  d2 <- out.df %>% select(-Hour) %>% 
    group_by(hour) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + #ylim(c(90, 120)) +
    ylab("Percent of Observed EDT crashes Estimated")
  
  paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
  labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
            "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")
  
  ggplot(d2, aes(x= hour, y= Pct_Obs_Est, fill = TotalObservedEDT)) +
    geom_line(aes(y = 100, x = 0:23), lwd = 1.5, col = "darkgreen") +
    geom_bar(stat="identity")+
    coord_polar()+
    ylim(c(0, 150)) + 
    ylab("") +
    xlab("Hour of Day") + 
    scale_y_continuous(labels = "", breaks = 1) +
    scale_x_continuous(labels = labs,
                       breaks= seq(0, 23, 2)) +
    ggtitle(paste(state, "Model", modelno,": Estimated EDT crashes / observed \n By hour of day"))
  ggsave(file = paste0(state, "_Obs_Est_", modelno, "_rose.jpg"), 
         device = 'jpeg', 
         path = file.path(localdir, "visualizations"), width = 7, height = 7, units = 'in')
  
  write.csv(out.df, file.path(outputdir, paste0(state, "_Output_Model_", modelno,".csv")), row.names = F)
  } # End model loop 
} # End state loop ----  

# Write out csv of model diagnostics

write.csv(counts, file = file.path(localdir, 'Random_Forest_Output', 'Multi-state_2019', 'Diagnostics.csv'),
          row.names = F)



SCRATCH = F

if(SCRATCH){
  
  d.day <- out.df %>% 
    group_by(DayOfWeek) %>%
    summarize(N = n(),
              TotalObservedEDT = sum(Obs),
              TotalEstimated = sum(Pred == "Crash"),
              Obs_Est_diff = TotalObservedEDT - TotalEstimated,
              Pct_Obs_Est = 100 *TotalEstimated / TotalObservedEDT)
  
  d.day # Compare with day of week table in Tableau
  
}
