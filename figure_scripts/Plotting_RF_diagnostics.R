# Plotting Precision/Recall tradeoff
# Using RF model 30 as basis

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(tidyverse)
library(rgdal)

codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = 1
codeloc <- "~/SDI_Waze" 
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR
outputdir <- file.path(localdir, 'Random_Forest_Output')

setwd(localdir)
# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# Load model outputs ----
# Get model 30 for each state
# Pull down model outputs from S3 if necessary
md.files <-  c(
  'MD_VMT_Output_to_30.RData',
  'MD_Model_30_RandomForest_Output.RData')
  
# MD files
#  system(paste("aws s3 ls", file.path(teambucket, 'MD/')))
for(i in md.files){
  if(length(grep(i, dir(outputdir))) == 0){
    system(paste("aws s3 cp",
                 file.path(teambucket, "MD", i),
                 file.path(outputdir, i)))
  }
}

ct.files <-  c(
  'CT_VMT_Output_to_30.RData',
  'CT_Model_30_RandomForest_Output.RData')

# CT files
#  system(paste("aws s3 ls", file.path(teambucket, 'CT/')))

for(i in ct.files){
  if(length(grep(i, dir(outputdir))) == 0){
    system(paste("aws s3 cp",
                 file.path(teambucket, "CT", i),
                 file.path(outputdir, i)))
  }
}

ut.files <-  c(
  'UT_VMT_Output_to_30.RData',
  'UT_Model_30_RandomForest_Output.RData')

# UT files
#  system(paste("aws s3 ls", file.path(teambucket, 'UT/')))
for(i in ut.files){
  if(length(grep(i, dir(outputdir))) == 0){
    system(paste("aws s3 cp",
                 file.path(teambucket, "UT", i),
                 file.path(outputdir, i)))
  }
}

va.files <-  c(
  'VA_VMT_Output_to_30.RData',
  'VA_Model_30_RandomForest_Output.RData')

# VA files
#  system(paste("aws s3 ls", file.path(teambucket, 'VA/')))
for(i in va.files){
  if(length(grep(i, dir(outputdir))) == 0){
    system(paste("aws s3 cp",
                 file.path(teambucket, "VA", i),
                 file.path(outputdir, i)))
  }
}

# Loop over states to produce tradeoff plots ----
states = c('CT', 'MD', 'UT', 'VA')
modelno = "30"
do.months = paste("2017", c("04","05","06","07","08","09"), sep="-")

for(state in states){ # state = 'CT'
  cat(rep("<>", 10), "\n", state, "\n", rep("<>", 10), "\n")
  load(file.path(outputdir, paste0(state, "_Model_", modelno, "_RandomForest_Output.RData")))
  
  # Load prepared input data. Run one of the RF scripts, such as RandomForest_WazeGrid_VMT_compare.R to create the preapred data for each state if not present in ~/workingdata. This has one object, w.allmonths, a data frame of all GRID ID x day x hour rows and all variable columns
  load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '.RData')))
  
  # RF inputs
  response.var = "MatchEDT_buffer_Acc"
  alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "year", "weekday", "vmt_time",
                 "uniqWazeEvents", "nWazeRowsInMatch", 
                 "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                 grep("EDT", names(w.allmonths), value = T))
  
  alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")
  
  alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")
  
  
  # variable importance ----
  pdf(file.path(localdir, "Figures", paste0(state, "_Variable_Importance_Model_", modelno, ".pdf")), width = 8, height = 8)
  
  varImpPlot(rf.out, 
             main = paste(state, "Model 30 Variable Importance"),
             bg = scales::alpha("midnightblue", 0.5),
             n.var = 15)
  
  dev.off()
  # Classification ----
  pdf(file.path(localdir, "Figures", paste0(state, "_Visualzing_classification_Model_", modelno, ".pdf")), width = 10, height = 10)
  
  out.df$CorrectPred = out.df$TN | out.df$TP
  levels(out.df$Obs) = c("Obs = NoCrash", "Obs = Crash")
  
  ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
    geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
    scale_y_continuous(limits=c(0,500), oob = scales::rescale_none) + 
    ggtitle("Probability of Waze event being categorized as EDT crash \n (Truncated at count = 500)") + 
    geom_vline(xintercept = 0.225, linetype = 'dotted') +
    annotate("text", x = 0.325, y = 500, label = "Cutoff = 0.225") +
    xlab("Estimated Crash Probability") +
    #theme_bw() + 
    scale_fill_brewer(palette="Set1")
  
  ggplot(out.df, aes(Prob.Crash, fill = CorrectPred)) + 
    geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
    scale_y_continuous(limits=c(0, 500), oob = scales::rescale_none) + 
    geom_vline(xintercept = 0.225, linetype = 'dotted') +
    facet_wrap(~Obs) +
    xlab("Estimated Crash Probability") +
    scale_fill_brewer(palette="Set1") +
    ggtitle("Frequency of classification as EDT crash by observed values \n (Truncated at count = 500; Max = 600,000)")
  
  # Plotting historgram of difference from observed and estimated, by grid cell aggregated over time ----
  levels(out.df$Pred) = c(1, 0) # from Crash, NoCrash
  levels(out.df$Obs) = c(0, 1) # from Obs  =NoCrash, Obs = Crash
  
  pct.diff.grid <- out.df %>%
    mutate(nObs = as.numeric(as.character(Obs)),
           nPred = as.numeric(as.character(Pred))) %>%
    group_by(GRID_ID) %>%
    summarize(sumObs = sum(nObs),
              sumPred = sum(nPred),
              Pct.diff = 100*(sumPred - sumObs) / sumObs)
  
  pct.diff.grid$Pct.diff[is.na(pct.diff.grid$Pct.diff) | pct.diff.grid$Pct.diff == Inf] = 0
  
  #hist(pct.diff.grid$Pct.diff)
  
  # Aggregate for report table
  pct.diff.table = as.data.frame(table(pct.diff.cut <- cut(pct.diff.grid$Pct.diff, breaks = c(-2000, -100, -50, -1, 0, 50, 100, 2000))))
  # https://drsimonj.svbtle.com/pretty-histograms-with-ggplot2. Trick is fill = cut()
  
  levels(cut(pct.diff.grid$Pct.diff, 25)) # create manual colors to match tablesu 
  
  ggplot(pct.diff.grid, aes(Pct.diff, fill = cut(Pct.diff, 
                                                 breaks = c(-1000, -100, -50, -25, 
                                                            0, 
                                                            25, 50, 100, 1000) ))) + 
    geom_histogram(bins = 10, show.legend = F, binwidth = 10) +
    theme_dark() +
    ggtitle("Summary of percent difference from observed and estimated EDT-level crashes") +
    xlab("Percent difference") + ylab("Frequency by GRID_ID") +
    scale_fill_brewer(palette = "RdBu", direction = -1)
  #
    #  scale_color_gradient(low = 'red', high = 'midnightblue')

  # Choosing cutoffs ----
  # Low value is most greedy for non-crashes, high value is more greedy for crashes
  omits = c(alwaysomit, alert_subtypes)
  fitvars <- names(w.allmonths)[is.na(match(names(w.allmonths), omits))]
  test.dat.use = w.allmonths[testrows,]
  reference.vec <- test.dat.use[,response.var]
  levels(reference.vec) = c("NoCrash", "Crash")
  
  co = seq(0.1, 0.9, by = 0.1)
  pt.vec <- vector()
  for(i in co){
    predx <- predict(rf.out, w.allmonths[testrows, fitvars], type = "response", cutoff = c(1-i, i))
    levels(predx) = c("NoCrash","Crash")
  
    predx <-as.factor(as.character(predx)) # Crash is first
    reference.vec <-as.factor(as.character(reference.vec)) # Same
  
    predtab <- table(predx, reference.vec, 
                      dnn = c("Predicted","Observed")) 
    if(sum(dim(predtab))==4) {bin.diag = bin.mod.diagnostics(predtab)} else {bin.diag = NA}
    
    pt.vec <- cbind(pt.vec, bin.diag)
    cat(i, ". ")
  }
  colnames(pt.vec) = co
  assign(paste0(state, '_pt.vec'), pt.vec)
  
  # High precision: minimize false positives. Achieved with the strictest requirement for classifying as a crash
  # High recall (sensitivity): minimize false negatives. Acheived with the least strict requrirement for classifiying as a crash
  
  # Try to make a nicer version in ggplot
  library(tidyr)
  
  prec.recall <- as.data.frame(pt.vec) %>%
    gather()
  
  prec.recall$Metric = rep(c(1, 3, 2, 4), ncol(pt.vec))
  prec.recall$Metric <- factor(prec.recall$Metric, labels = c("Accuracy", "Recall","Precision", "False Positive Rate"))
  names(prec.recall)[1:2] = c("Cutoff", "Value")
  
  ggplot(prec.recall, aes(x = Cutoff, y = Value, group = Metric)) + 
    geom_line(aes(color = Metric), size = 2) +
    ggtitle(paste0(state, "Crash classification tradeoffs, April-September 2017 \n Model ", modelno)) + 
    theme_bw() +
    annotate("text",
             x = 0.5,
             y = c(0.95, 0.05, 0.46,0.81),
             hjust = 0,
             label = c("Accuracy", "False Positive Rate", "Precision", "Recall"))
               
  
  dev.off()
  

} # end state loop ----

fn = file.path(outputdir, paste0("Cutoff_calcs_Model_", modelno, "_", Sys.Date()))
save(list = paste0(states, "_pt.vec"), file = fn)





# Scratch: using pROC ----

SCRATCH = F
if(SCRATCH){

plot(pROC::roc(out.df$Obs, out.df$Prob.Crash, auc = TRUE))

(model_auc <- pROC::auc(out.df$Obs, out.df$Prob.Crash))

plot(pROC::roc(out.df$Obs, out.df$Prob.Crash, auc = TRUE))




# From Fx
(model_auc <- pROC::auc(test.dat.use[,response.var], rf.prob[,colnames(rf.prob)=="1"]))

identical(out.df$Obs, test.dat.use[,response.var])
head(data.frame(out.df$Obs, test.dat.use[,response.var]))
class(out.df$Obs)
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

plot(pROC::roc(out.df$Obs, rf.04.prob2[,colnames(rf.04.prob2)=="1"], auc = TRUE))

reference.vec <- w.04$MatchEDT_buffer_Acc[testrows]
levels(reference.vec) = c("NoCrash", "Crash")
levels(rf.04.pred2) = c("NoCrash","Crash")

reference.vec <-as.factor(as.character(reference.vec))
rf.04.pred2 <-as.factor(as.character(rf.04.pred2))

(predtab <- table(rf.04.pred2, reference.vec, 
                  dnn = c("Predicted","Observed"))) 
bin.mod.diagnostics(predtab)
}
