# Plotting Precision/Recall tradeoff
# Can select RF model, e.g. modelno = "30" 

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

# Loop over states to produce tradeoff plots ----
# <><><><><><><>
states = c('CT', 'MD', 'UT', 'VA')
modelno = 30 # "18" # "30"
do.months = paste("2018", formatC(1:12, width = 2, flag = 0), sep="-")
# <><><><><><><>

counts = vector() # To store confusion matrix outputs

for(state in states){ # state = 'CT'
  cat("\n", rep("<>", 10), "\n", state, "\n\n")
  
  # Grab from S3 if necessary
  s3.files <-  c(
    #paste0(state, '_VMT_Output_to_', modelno, '.RData'),
    paste0(state, '_Model_', modelno,'_RandomForest_Output.RData'))
  
  # State files
  #  system(paste("aws s3 ls", paste0(teambucket,'/', state, '/')))
  for(i in s3.files){
    if(length(grep(i, dir(outputdir))) == 0){
      system(paste("aws s3 cp",
                   file.path(teambucket, state, i),
                   file.path(outputdir, i)))
    }
  }
  
  load(file.path(outputdir, paste0(state, "_Model_", modelno, "_RandomForest_Output.RData")))
  
  # Load prepared input data. Run one of the RF scripts, such as RandomForest_WazeGrid_VMT_compare.R to create the preapred data for each state if not present in ~/workingdata. This has one object, w.allmonths, a data frame of all GRID ID x day x hour rows and all variable columns
  load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_2018.RData')))
  
  # RF inputs
  response.var = "MatchEDT_buffer_Acc"
  alwaysomit = c(grep("GRID_ID", names(w.allmonths), value = T), "day", "hextime", "Year", "weekday", 
                 "ymd", "month", "vmt_time",
                 "uniqueWazeEvents", "nWazeRowsInMatch", 
                 "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                 grep("EDT", names(w.allmonths), value = T))
  
  
  alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")
  
  alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")
  
  # Get columns right
  out.df = out.df[1:8]
  names(out.df)[1:8] = c('GRID_ID', 'Year', 'day', 'hour',
                    'Obs', 'Pred', 'Prob.Noncrash', 'Prob.Crash')
  out.df = data.frame(out.df,
                      TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                      FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                      FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                      TP = out.df$Obs == 1 &  out.df$Pred == "Crash")
  
  # variable importance ----
  pdf(file.path(localdir, "Figures", paste0(state, "_Variable_Importance_Model_", modelno, ".pdf")), width = 8, height = 8)
  
  varImpPlot(rf.out, 
             main = paste(state, "Model", modelno, "Variable Importance"),
             bg = scales::alpha("midnightblue", 0.5),
             n.var = 15)
  
  dev.off()
  # Classification ----
  pdf(file.path(localdir, "Figures", paste0(state, "_Visualzing_classification_Model_", modelno, ".pdf")), width = 10, height = 10)
  
  out.df$CorrectPred = out.df$TN | out.df$TP
  levels(out.df$Obs) = c("Obs = NoCrash", "Obs = Crash")
  
  gp1 <- ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
    geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
    scale_y_continuous(limits=c(0,500), oob = scales::rescale_none) + 
    ggtitle("Probability of Waze event being categorized as EDT crash \n (Truncated at count = 500)") + 
    geom_vline(xintercept = 0.225, linetype = 'dotted') +
    annotate("text", x = 0.325, y = 500, label = "Cutoff = 0.225") +
    xlab("Estimated Crash Probability") +
    #theme_bw() + 
    scale_fill_brewer(palette="Set1")
  
  print(gp1)
  
  gp2 <- ggplot(out.df, aes(Prob.Crash, fill = CorrectPred)) + 
    geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
    scale_y_continuous(limits=c(0, 500), oob = scales::rescale_none) + 
    geom_vline(xintercept = 0.225, linetype = 'dotted') +
    facet_wrap(~Obs) +
    xlab("Estimated Crash Probability") +
    scale_fill_brewer(palette="Set1") +
    ggtitle("Frequency of classification as EDT crash by observed values \n (Truncated at count = 500; Max = 600,000)")
  
  print(gp2)
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
  
  # levels(cut(pct.diff.grid$Pct.diff, 25)) # create manual colors to match tablesu 
  
  gp3 <- ggplot(pct.diff.grid, aes(Pct.diff, fill = cut(Pct.diff, 
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

  print(gp3)
  
  # Choosing cutoffs ----
  # Low value is most greedy for non-crashes, high value is more greedy for crashes
  fitvars <- rownames(rf.out$importance)
  test.dat.use = w.allmonths[testrows,]
  reference.vec <- test.dat.use[,response.var]
  levels(reference.vec) = c("NoCrash", "Crash")
  
#  co = seq(0.1, 0.9, by = 0.1)
  co = c(seq(0.025, 0.5, by = 0.025),
         seq(0.6, 0.9, by = 0.1))
  
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
  pt.vec = as.data.frame(pt.vec); colnames(pt.vec) = co
  prec.recall <- pt.vec %>%
    gather()
  
  prec.recall$Metric = rep(c(1, 3, 2, 4), ncol(pt.vec))
  prec.recall$Metric <- factor(prec.recall$Metric, labels = c("Accuracy", "Recall","Precision", "False Positive Rate"))
  names(prec.recall)[1:2] = c("Cutoff", "Value")

  assign(paste0(state, '_pt.vec'), prec.recall)
  
  # High precision: minimize false positives. Achieved with the strictest requirement for classifying as a crash
  # High recall (sensitivity): minimize false negatives. Acheived with the least strict requrirement for classifiying as a crash
  
  # Try to make a nicer version in ggplot
  library(tidyr)

  gp4 <- ggplot(prec.recall, aes(x = Cutoff, y = Value, group = Metric)) + 
    geom_line(aes(color = Metric), size = 2) +
    ggtitle(paste0(state, "Crash classification tradeoffs, April-September 2017 \n Model ", modelno)) + 
    theme_bw() +
    annotate("text",
             x = 0.5,
             y = c(0.95, 0.05, 0.46,0.81),
             hjust = 0,
             label = c("Accuracy", "False Positive Rate", "Precision", "Recall"))
  print(gp4)      
  
  dev.off()
  
  
  pt.vec = pt.vec[,!is.na(colSums(pt.vec))]
  recall_prec_diff = pt.vec['recall',]-pt.vec['precision',]
  
  best_cutoff = as.numeric(names(recall_prec_diff[which(abs(recall_prec_diff)==min(abs(recall_prec_diff)))][1]))
  # After setting cutoffs, run this:
  
  out.df$Pred = ifelse(out.df$Prob.Crash >= best_cutoff, 'Crash', 'NoCrash')
  
  out.df = data.frame(out.df[1:8],
                      TN = out.df$Obs == 0 &  out.df$Pred == "NoCrash",
                      FP = out.df$Obs == 0 &  out.df$Pred == "Crash",
                      FN = out.df$Obs == 1 &  out.df$Pred == "NoCrash",
                      TP = out.df$Obs == 1 &  out.df$Pred == "Crash"
  )
  
  w.group <- data.frame(TN = out.df$TN, TP = out.df$TP, FP = out.df$FP, FN = out.df$FN)
  
  w.group$TN[w.group$TN==TRUE] = "TN"
  w.group$TP[w.group$TP==TRUE] = "TP"
  w.group$FP[w.group$FP==TRUE] = "FP"
  w.group$FN[w.group$FN==TRUE] = "FN"
  
  w.group$group <- apply(w.group, 1, function(x) x[x!=FALSE][1])
  
  out.df$Pred.grp <- as.factor(w.group$group)
  
  grp.count = out.df %>%
    summarize(TN = sum(TN),
              FP = sum(FP),
              FN = sum(FN),
              TP = sum(TP))
  
  predtab <- table(out.df$Pred, reference.vec, 
                   dnn = c("Predicted","Observed")) 
  if(sum(dim(predtab))==4 ) {bin.diag = bin.mod.diagnostics(predtab)} else {bin.diag = NA}
  
  # plot(pROC::roc(out.df$Obs, out.df$Prob.Crash, auc = TRUE))
  
  (model_auc <- pROC::auc(out.df$Obs, out.df$Prob.Crash))
  
  grp.count = data.frame(grp.count, t(bin.diag), AUC = as.numeric(model_auc))
  
  counts = rbind(counts, grp.count)
  write.csv(out.df, file = file.path(outputdir, paste0(paste(state, 'Refit', modelno, sep = "_"), '.csv')), row.names=F)
  

} # end state loop ----

write.csv(counts, file = file.path(outputdir, paste0("Multistate_Confusion_matrix_counts_", modelno, ".csv")))

fn = file.path(outputdir, paste0("Cutoff_calcs_Model_", modelno, "_", Sys.Date()))
save(list = paste0(states, "_pt.vec"), file = fn)


# Save and export re-fit models ----

# Add nMatch, Match, nWazeAccident, date hour formatted

for(state in states){ # state = 'CT'
  refit = read.csv(file.path(outputdir, paste0(state, '_Refit_', modelno, '.csv')))
  load(file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_2018.RData')))
  
  addvars = c('ymd', 'month', 'weekday', 'uniqueWazeEvents', 'nMatchEDT_buffer', 'nWazeAccident', 'hextime')
  
  refit$day = as.character(refit$day)
  w.allmonths$day = as.character(w.allmonths$day)

  refit$GRID_ID = as.character(refit$GRID_ID)
  w.allmonths$GRID_ID = as.character(w.allmonths$GRID_ID)

  refit$hour = as.character(refit$hour)
  w.allmonths$hour = as.character(w.allmonths$hour)
  
  refit = left_join(refit, w.allmonths %>% select(c('GRID_ID',  'day', 'hour', addvars)), by = c('GRID_ID',  'day', 'hour'))
  write.csv(refit, file.path(outputdir, paste0(state, '_Refit_for_Tableau_', modelno, '.csv')), row.names = F)
}


# Make new hour-wheels

for(state in states){
  dd <- read.csv(file.path(outputdir, paste0(state, '_Refit_for_Tableau_', modelno, '.csv')))
  
    d2 <- dd %>% 
    group_by(hour) %>%
    summarize(N = n(),
              TotalWazeAcc = sum(nWazeAccident, na.rm = T),
              TotalObserved = sum(Obs, na.rm = T),
              TotalEstimated = sum(Pred == 'Crash'),
              Obs_Est_diff = TotalObserved - TotalEstimated,
              Pct_Obs_Est = 100 * TotalEstimated / TotalObserved)
  
  ggplot(d2, aes(x = hour, y = Pct_Obs_Est)) + geom_line() + ylim(c(70, 120)) +
    ylab("Percent of Observed TN crashes Estimated")
  
  paste(rep(seq(0, 12, 2), 2), c("AM", "PM"))
  labs <- c("12 AM", "2 AM", "4 AM", "6 AM", "8 AM", "10 AM",
            "12 PM", "2 PM", "4 PM", "6 PM", "8 PM", "10 PM")
  
  ggplot(d2, aes(x= hour, y= 100
                 , fill = Pct_Obs_Est)) +
   # geom_line(aes(y = 100, x = 0:23), lwd = 1, col = "grey20") +
    geom_bar(stat="identity")+
    coord_polar(clip = 'off')+
    ylab("") +
    xlab("Hour of Day") + 
    scale_y_continuous(labels = "", breaks = 1) +
   # scale_fill_gradientn(colors = heat.colors(10)) +
   # scale_fill_brewer(palette = 'RdBu') 
    scale_fill_gradient2(high = 'tomato', # scales::muted(
                         midpoint = 100, mid = 'lightblue', 
                         low = 'darkblue',
                         space = 'Lab') +
    scale_x_continuous(labels = labs, 
                       breaks= seq(0, 23, 2)) +
    theme_bw() +
    ggtitle(paste(state, "Model", modelno, ": Estimated crashes / observed \n By hour of day"))
  
  ggsave(file = paste0(state, "_Obs_Est_", modelno, "_rose.jpg"), device = 'jpeg', 
         path = "~/workingdata/Figures", 
         width = 7, height = 7, units = 'in')
  
}

# Save to team bucket 
zipname = paste0('MultiState_Tableau_Out', "_", Sys.Date(), '.zip')

outfiles = c('CT_Refit_for_Tableau_30.csv',
             'MD_Refit_for_Tableau_30.csv',
             'UT_Refit_for_Tableau_30.csv',
             'VA_Refit_for_Tableau_30.csv')

figfiles = c('CT_Obs_Est_30_rose.jpg',
             'MD_Obs_Est_30_rose.jpg',
             'UT_Obs_Est_30_rose.jpg',
             'VA_Obs_Est_30_rose.jpg')

system(paste('zip -j', file.path('~/workingdata', zipname),
             paste(file.path(outputdir, outfiles), collapse = " "),
             paste(file.path(localdir, 'Figures', figfiles), collapse = " "))
)

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


t# Scratch: using pROC ----

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
