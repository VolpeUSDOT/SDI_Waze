library(data.table)
library(pROC)
library(caret)
library(randomForest)
library(ggplot2)
library(plotROC)
library(ROCR)
library(aws.s3)



codeloc <- "~/SDI_Waze" 

source(file.path(codeloc, 'utility/wazefunctions.R'))

waze.bucket <- "ata-waze"


HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids

dir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")
aws_contents <- system("aws s3 ls s3://ata-waze/", dir, "/")

for(i in aws_contents) == 0){
  
  s3transfer = paste("aws s3 ls s3://ata-waze/", dir, "/")
  system(s3transfer)
  
}
# Model 17
# April + May, predict June, 1 mi, neighbors, wx, roads, jobs ----

#Reloading model and recreating needed variables to rerun predictions
load("C:/Users/graham.robart/Documents/Model_17_RandomForest_Output_0405_06.RData")

omits = c(grep("GRID_ID", names(w.06), value = T), "day", "hextime", "year", "weekday",
          "uniqWazeEvents", "nWazeRowsInMatch", "nWazeAccident",
          "nMatchWaze_buffer", "nNoMatchWaze_buffer",
          grep("EDT", names(w.06), value = T))

fitvars <- names(w.06)[is.na(match(names(w.06), omits))]


#Predictions, modified to use probabilities in addition to discrete predictions

model.17.pred <- predict(rf.0405.all, w.06[, fitvars])
model.17.prob <- predict(rf.0405.all, w.06[, fitvars], type = "prob")

out.17 <- data.table(w.06[c("GRID_ID","day","hour", "MatchEDT_buffer")], model.17.pred, model.17.prob)
names(out.17)[4:7] <- c("Obs", "Pred", "Prob.True", "Prob.False")

##################
# ROC generation #
##################

#basic plot


#using pROC
plot(roc(out.17$Obs, out.17$Prob.True, auc = TRUE))


#Using ROCR
roc_perf <- performance(prediction(out.17$Prob.False, out.17$Obs), 'tpr', 'fpr')
plot(roc_perf)

#ggplot attempt (not working right now)
Model_ROC <- ggplot(performance(prediction(out.17$Prob.False, out.17$Obs), 'tpr', 'fpr'), title = paste("Model", modelno, "ROC")) + 
  labs(title= "ROC curve", 
       x = "False Positive Rate (1-Specificity)", 
       y = "True Positive Rate (Sensitivity)") +
  geom_line() 
Model_ROC


