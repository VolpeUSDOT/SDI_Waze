# Lasso logistic regression work for Waze

# Goals: produce logistic regression analysis for features of Waze accidents and other event types that predict matching with EDT reports 

# Running on gridded data, with Waze predictors only. Try three versions. For each one, produce confusion matrix, binary model diagnostics, and output .csv of grid IDs and four columns, one each for true negative, false postitive, false negative, true positive.

# 1. April, 70/30 split
# 2. April + May, 70 / 30 split
# 3. April + May, predict June.

# After this is complete, repeat these three with Waze accidents and nMatchEDT_acc as response only
# If only Waze accident report
# 1. Predict to nMatchEDT_buffer_Acc (binary version, MatchEDT_buffer_Acc)
# 2. Subset to only nWazeAccident > 1 ? look at this. 

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(speedglm)
library(CORElearn)
library(glmnet)
library(foreach) 


setwd("~/")

#Sudderth drive
#if(length(grep("EASdocs", getwd())) > 0) {mappeddrive = "S:"} 
mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load
codeloc <- "~/GitHub/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

#Flynn drive
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} 
codeloc <- "~/git/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT"))
#wazedir <- "~/Temp Working Docs/SDI_temp" # Dan local
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")

setwd(wazedir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Read in data, renaming data files by month. For each month, prep time and response variables

load("WazeTimeEdtHex_04.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)

# Going to binary for all Waze buffer match:
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)

# Going to binary for all Waze Accident buffer match:

wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.04 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeTimeEdtHex_05.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)
wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.05 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

load("WazeTimeEdtHex_06.RData")
wazeTime.edt.hex$DayOfWeek <- as.factor(wazeTime.edt.hex$DayOfWeek)
wazeTime.edt.hex$hour <- as.numeric(wazeTime.edt.hex$hour)
wazeTime.edt.hex$MatchEDT_buffer <- wazeTime.edt.hex$nMatchEDT_buffer
wazeTime.edt.hex$MatchEDT_buffer[wazeTime.edt.hex$MatchEDT_buffer > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer <- as.factor(wazeTime.edt.hex$MatchEDT_buffer)
wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

wazeTime.edt.hex$MatchEDT_buffer_Acc <- wazeTime.edt.hex$nMatchEDT_buffer_Acc
wazeTime.edt.hex$MatchEDT_buffer_Acc[wazeTime.edt.hex$MatchEDT_buffer_Acc > 0] = 1 
wazeTime.edt.hex$MatchEDT_buffer_Acc <- as.factor(wazeTime.edt.hex$MatchEDT_buffer_Acc)

w.06 <- wazeTime.edt.hex; rm(wazeTime.edt.hex) 

# Exploration of response variable: For April, only 1,600 out of 310,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater. 15,000 have 1.
# summary(w.05$nMatchEDT_buffer > 1)
# table(w.05$nMatchEDT_buffer[w.05$nMatchEDT_buffer > 1])


# Analysis ----

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well
#All Waze matches
fitvars <- names(w.04)[is.na(match(names(w.04),
                                             c("GRID_ID", "day", # place variables to omit as predictors in this vector 
                                               "nMatchWaze_buffer", "nNoMatchWaze_buffer", "DayOfWeek",
                                               grep("EDT", names(w.04), value = T)
                                               )))]


wazeformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                              "MatchEDT_buffer"))], 
                           response = "MatchEDT_buffer")


wazeAccformula <- reformulate(termlabels = fitvars[is.na(match(fitvars,
                                                            "MatchEDT_buffer_Acc"))], 
                           response = "MatchEDT_buffer_Acc")

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Lasso

# problem: 0 for DayofWeek for EDT-only, omit for now
w.04 <- w.04[w.04$DayOfWeek != 0 & !is.na(w.04$MatchEDT_buffer),]
w.04$DayOfWeek <- as.factor(as.character(w.04$DayOfWeek))
# will need to convert to dummy variables for lasso, cannot handle categorical input

# Model 1: April, 70/30 ----

trainrows <- sort(sample(1:nrow(w.04), size = nrow(w.04)*.7, replace = F))
testrows <- (1:nrow(w.04))[!1:nrow(w.04) %in% trainrows]
 
X <- as.matrix(w.04[trainrows, fitvars])

X = as(X, "sparseMatrix") # saves ~ 50% of the storage

Y = as.matrix(w.04$MatchEDT_buffer[trainrows])

starttime <- Sys.time()
glmnet.fit <- glmnet(x = X,
                     y = Y,
                     family = "binomial")

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
# 1 min with sparse matrix


plot(glmnet.fit, label = T, xvar = "dev")

# Cross-validation. Try both 'class' for misclassification rate an 'auc' for area under ROC.
# Use this step to identify optimal lambda . Time- and CPU intensive, ~ 10 min for w.04
# To do: make parallel, as in this example, or by simply specifying parallel = T
# model_fit <- foreach(ii = seq_len(ncol(target))) %dopar% {
#   cv.glmnet(x, target[,ii], family = "binomial", alpha = 0,
#             type.measure = "auc", grouped = FALSE, standardize = FALSE,
#             parallel = TRUE)
# }

starttime <- Sys.time()
cv.fit <- cv.glmnet(x = X,
                    y = Y,
                    family = "binomial",
                    type.measure = "class")

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
plot(cv.fit) # to see the misclassification error by lambda.
cv.fit$lambda.min; cv.fit$lambda.1se # 0.0002393555

X.pred <- as(as.matrix(w.04[testrows, fitvars]), "sparseMatrix")

glmnet.04.pred <- predict(glmnet.fit, 
                      newx = X.pred,
                      type = "class",
                      s = cv.fit$lambda.1se)

Nobs <- data.frame(t(c(nrow(w.04),
               summary(w.04$MatchEDT_buffer),
               length(w.04$nWazeAccident[w.04$nWazeAccident>0]) 
               )))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.04$MatchEDT_buffer[testrows], glmnet.04.pred)) 
bin.mod.diagnostics(predtab)

# save output predictions

out.04 <- data.frame(w.04[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], glmnet.04.pred)
out.04$day <- as.numeric(out.04$day)
names(out.04)[4:5] <- c("Obs", "Pred")

out.04 = data.frame(out.04,
                    TN = out.04$Obs == 0 &  out.04$Pred == 0,
                    FP = out.04$Obs == 0 &  out.04$Pred == 1,
                    FN = out.04$Obs == 1 &  out.04$Pred == 0,
                    TP = out.04$Obs == 1 &  out.04$Pred == 1)
write.csv(out.04,
          file = "Lasso_pred_04.csv",
          row.names = F)
                    

coefs <- coef(glmnet.fit, s = cv.fit$lambda.1se)
OR <- data.frame(OR = exp(coefs[order(abs(coefs[,1]), decreasing = T),])) # Odds ratios of coefficients. EDT events occur about 0.027 of the time; this is 3x more likely with each additional Waze accident.


save(list = c("glmnet.fit",
              "cv.fit",
              "testrows",
              "trainrows",
              "w.04",
              "OR",
              "out.04"),
     file = "Lasso_Output_04.RData")

# Model 2: April + May, 70/30 ----

w.0405 <- rbind(w.04, w.05)

trainrows <- sort(sample(1:nrow(w.0405), size = nrow(w.0405)*.7, replace = F))
testrows <- (1:nrow(w.0405))[!1:nrow(w.0405) %in% trainrows]

X <- as.matrix(w.0405[trainrows, fitvars])

X = as(X, "sparseMatrix") # saves ~ 50% of the storage

Y = as.matrix(w.0405$MatchEDT_buffer[trainrows])

starttime <- Sys.time()
glmnet.fit <- glmnet(x = X,
                     y = Y,
                     family = "binomial")

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
# 2.3 min 

plot(glmnet.fit, label = T, xvar = "dev")

# Cross-validation. Try both 'class' for misclassification rate an 'auc' for area under ROC.
# Use this step to identify optimal lambda . Time- and CPU intensive, ~ 23 min for w.0405
starttime <- Sys.time()
cv.fit <- cv.glmnet(x = X,
                    y = Y,
                    family = "binomial",
                    type.measure = "class")

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
plot(cv.fit) # to see the misclassification error by lambda.
cv.fit$lambda.min; cv.fit$lambda.1se # 

X.pred <- as(as.matrix(w.0405[testrows, fitvars]), "sparseMatrix")

glmnet.0405.pred <- predict(glmnet.fit, 
                          newx = X.pred,
                          type = "class",
                          s = cv.fit$lambda.1se)

Nobs <- data.frame(t(c(nrow(w.0405),
                       summary(w.0405$MatchEDT_buffer),
                       length(w.0405$nWazeAccident[w.0405$nWazeAccident>0]) 
)))

colnames(Nobs) = c("N", "No EDT", "EDT present", "Waze accident present")
format(Nobs, big.mark = ",")

(predtab <- table(w.0405$MatchEDT_buffer[testrows], glmnet.0405.pred)) 
bin.mod.diagnostics(predtab)

# save output predictions

out.0405 <- data.frame(w.0405[testrows, c("GRID_ID", "day", "hour", "MatchEDT_buffer")], glmnet.0405.pred)
out.0405$day <- as.numeric(out.0405$day)
names(out.0405)[4:5] <- c("Obs", "Pred")

out.0405 = data.frame(out.0405,
                    TN = out.0405$Obs == 0 &  out.0405$Pred == 0,
                    FP = out.0405$Obs == 0 &  out.0405$Pred == 1,
                    FN = out.0405$Obs == 1 &  out.0405$Pred == 0,
                    TP = out.0405$Obs == 1 &  out.0405$Pred == 1)
write.csv(out.0405,
          file = "Lasso_pred_0405.csv",
          row.names = F)

coefs <- coef(glmnet.fit, s = cv.fit$lambda.1se)
(OR <- data.frame(OR = exp(coefs[order(abs(coefs[,1]), decreasing = T),])) ) # Odds ratios of coefficients. 


save(list = c("glmnet.fit",
              "cv.fit",
              "testrows",
              "trainrows",
              "w.0405",
              "out.0405"),
     file = "Lasso_Output_0405.RData")


# Model 3: April + May, predict June ----

X <- as.matrix(w.0405[fitvars])

X = as(X, "sparseMatrix") 

Y = as.matrix(w.0405$MatchEDT_buffer)

starttime <- Sys.time()
glmnet.fit.06 <- glmnet(x = X,
                     y = Y,
                     family = "binomial")

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
# X min 

plot(glmnet.fit.06, label = T, xvar = "dev")

# Cross-validation. 
# Time: 
starttime <- Sys.time()
cv.fit.06 <- cv.glmnet(x = X,
                    y = Y,
                    family = "binomial",
                    type.measure = "class",
                    parallel = T)

timediff <- round(Sys.time()-starttime, 2)
cat("complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
plot(cv.fit.06) # to see the misclassification error by lambda.
cv.fit.06$lambda.min; cv.fit.06$lambda.1se # 

X.pred <- as(as.matrix(w.06[fitvars]), "sparseMatrix")

glmnet.06.pred <- predict(glmnet.fit.06, 
                          newx = X.pred,
                          type = "class",
                          s = cv.fit.06$lambda.1se)


(predtab <- table(w.06$MatchEDT_buffer, glmnet.06.pred)) 
bin.mod.diagnostics(predtab)

# save output predictions

out.06 <- data.frame(w.06[c("GRID_ID", "day", "hour", "MatchEDT_buffer")], glmnet.06.pred)
out.06$day <- as.numeric(out.06$day)
names(out.06)[4:5] <- c("Obs", "Pred")

out.06 = data.frame(out.06,
                    TN = out.06$Obs == 0 &  out.06$Pred == 0,
                    FP = out.06$Obs == 0 &  out.06$Pred == 1,
                    FN = out.06$Obs == 1 &  out.06$Pred == 0,
                    TP = out.06$Obs == 1 &  out.06$Pred == 1)
write.csv(out.06,
          file = "Lasso_pred_06.csv",
          row.names = F)

coefs <- coef(glmnet.fit.06, s = cv.fit.06$lambda.1se)
(OR <- data.frame(OR = exp(coefs[order(abs(coefs[,1]), decreasing = T),])) ) # Odds ratios of coefficients. 

save(list = c("glmnet.fit.06",
              "cv.fit.06",
              "OR",
              "testrows",
              "trainrows",
              "w.06",
              "out.06"),
     file = "Lasso_Output_06.RData")



# For Waze accidents only ----

# to do... will need to resolve same issues as for RF work, namely do we subset for nWazeAccidents > 0 or just predict on MatchEDT_buffer_Acc.