# Plotting Precision/Recall tradeoff
# Using RF model 30 as basis

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(aws.s3)
library(tidyverse)
library(rgdal)

codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = 1
inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

aws.signature::use_credentials()
waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)
# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# Load model output ----
modelno = "30"
s3load(object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output.RData", sep= "_")), bucket = waze.bucket)

# variable importance

varImpPlot(rf.out, 
           main = "Model 30 Variable Importance",
           bg = scales::alpha("midnightblue", 0.5),
           n.var = 15)

imp30 <- rf.out$importance

imp30 <- as.data.frame(imp30)

imp30$var = rownames(imp30)

imp30 <- imp30[order(imp30$MeanDecreaseGini, decreasing = T),]

# ggplot(imp30, aes(y=MeanDecreaseGini, x=var)) + geom_bar(stat="identity")

# Classification ----
pdf(paste0("Visualzing_classification_Model_", modelno, ".pdf"))

out.df$CorrectPred = out.df$TN | out.df$TP
levels(out.df$Obs) = c("Obs = NoCrash", "Obs = Crash")

# ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
#   geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.1) +
#   ggtitle("Probability of Waze event being categorized as EDT crash")

ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  scale_y_continuous(limits=c(0,500), oob = scales::rescale_none) + 
  ggtitle("Probability of Waze event being categorized as EDT crash \n (Truncated at count = 500)") + 
  geom_vline(xintercept = 0.225, linetype = 'dotted') +
  annotate("text", x = 0.325, y = 500, label = "Cutoff = 0.225") +
  xlab("Estimated Crash Probability") +
  #theme_bw() + 
  scale_fill_brewer(palette="Set1")

# ggplot(out.df, aes(Prob.Crash, fill = Pred)) + 
#   geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
#   scale_y_continuous(limits=c(0,500), oob = scales::rescale_none) + 
#   ggtitle("Classification Waze event as EDT crash \n (Truncated at count = 500)")

ggplot(out.df, aes(Prob.Crash, fill = CorrectPred)) + 
  geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
  scale_y_continuous(limits=c(0, 500), oob = scales::rescale_none) + 
  geom_vline(xintercept = 0.225, linetype = 'dotted') +
  facet_wrap(~Obs) +
  xlab("Estimated Crash Probability") +
  scale_fill_brewer(palette="Set1") +
  ggtitle("Frequency of classification as EDT crash by observed values \n (Truncated at count = 500; Max = 600,000)")

# ggplot(out.df, aes(Prob.Crash, fill = Obs)) + 
#   geom_histogram(alpha = 0.5, position = "identity", binwidth = 0.01) + 
#   scale_y_continuous(limits=c(0,500), oob = scales::rescale_none) + 
#   facet_wrap(~Pred) +
#   ggtitle("Classification Waze event as EDT crash by predicted values \n (Truncated at count = 500)")

# Plotting historgram of difference from observed and estimated, by grid cell aggregated over time ----
s3load(object = file.path(outputdir, paste("Model", modelno, "RandomForest_Output.RData", sep= "_")), bucket = waze.bucket)
levels(out.df$Pred) = c(1, 0) # from Crash, NoCrash

pct.diff.grid <- out.df %>%
  mutate(nObs = as.numeric(as.character(Obs)),
         nPred = as.numeric(as.character(Pred))) %>%
  group_by(GRID_ID) %>%
  summarize(sumObs = sum(nObs),
            sumPred = sum(nPred),
            Pct.diff = 100*(sumPred - sumObs) / sumObs)

pct.diff.grid$Pct.diff[is.na(pct.diff.grid$Pct.diff) | pct.diff.grid$Pct.diff == Inf] = 0

hist(pct.diff.grid$Pct.diff)


# Aggregate for report table

as.data.frame(table(pct.diff.cut <- cut(pct.diff.grid$Pct.diff, breaks = c(-2000, -100, -50, -1, 0, 50, 100, 2000))))

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

  
# 
  scale_fill_manual(values = c("cadetblue4", "cadetblue2", "cadetblue1",
                               "chocolate1", "chocolate2", "chocolate3", "chocolate4",
                               "chocolate4", "chocolate4", "chocolate4"))



# Choosing cutoffs. ----
# Low value is most greedy for non-crashes, high value is more greedy for crashes
# Load w.04_09 by running firs 100 lines of RandomForest_WazeGrid_Full.R
omits = c(alwaysomit, alert_subtypes)
fitvars <- names(w.04_09)[is.na(match(names(w.04_09), omits))]
test.dat.use = w.04_09[testrows,]
reference.vec <- test.dat.use[,response.var]
class(reference.vec) <- "data.frame"
reference.vec <- as.factor(reference.vec[,1])
levels(reference.vec) = c("NoCrash", "Crash")

co = seq(0.1, 0.9, by = 0.1)
pt.vec <- vector()
for(i in co){
  predx <- predict(rf.out, w.04_09[testrows, fitvars], type = "response", cutoff = c(1-i, i))
  levels(predx) = c("NoCrash","Crash")

  predx <-as.factor(as.character(predx)) # Crash is first
  reference.vec <-as.factor(as.character(reference.vec)) # Same

  predtab <- table(predx, reference.vec, 
                    dnn = c("Predicted","Observed")) 
  bin.mod.diagnostics(predtab)
  
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
title(main = "Waze-EDT crash classification \n April-September 2017 Maryland \n Model 30")
# High precision: minimize false positives. Achieved with the strictest requirement for classifying as a crash
# High recall (sensitivity): minimize false negatives. Acheived with the least strict requrirement for classifiying as a crash
# Recommended threshold: 0.2 for crash (0.8 for non-crash)


# Try to make a nicer version in ggplot
library(tidyr)

prec.recall <- as.data.frame(pt.vec) %>%
  gather()

prec.recall$Metric = rep(c(1, 3, 2, 4), ncol(pt.vec))
prec.recall$Metric <- factor(prec.recall$Metric, labels = c("Accuracy", "Recall","Precision", "False Positive Rate"))
names(prec.recall)[1:2] = c("Cutoff", "Value")

ggplot(prec.recall, aes(x = Cutoff, y = Value, group = Metric)) + 
  geom_line(aes(color = Metric), size = 2) +
  ggtitle("Crash classification tradeoffs, April-September 2017 Maryland \n Model 30") + 
  theme_bw() +
  annotate("text",
           x = 0.5,
           y = c(0.95, 0.05, 0.46,0.81),
           hjust = 0,
           label = c("Accuracy", "False Positive Rate", "Precision", "Recall"))
             

dev.off()




#using pROC
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
