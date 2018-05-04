# Lasso logistic regression work for Waze

# Goals: produce logistic regression analysis for features of Waze accidents and other event types that predict matching with EDT reports 

# Run on gridded data for March-Sept 2017 MD data.
#Include data features based on the RF analysis

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(speedglm)
library(CORElearn)
library(glmnet)
library(foreach) 


#Data prep
codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
do.months = c("04","05","06","07","08","09")

inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_Lasso_Output")

aws.signature::use_credentials()
waze.bucket <- "ata-waze"
localdir <- "/home/dflynn-volpe/workingdata" 

setwd(localdir)
# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# read random forest function
source(file.path(codeloc, "analysis/RandomForest_WazeGrid_Fx.R"))

# check if already complted this transfer on this instance
if(length(dir(localdir)[grep("shapefiles_funClass", dir(localdir))]) == 0){
  
  s3transfer = paste("aws s3 cp s3://ata-waze/MD_hexagon_shapefiles", localdir, "--recursive --include '*'")
  system(s3transfer)
  
  for(dd in c("shapefiles_funClass.zip", "shapefiles_rac.zip", "shapefile_wac.zip", "shapefiles_AADT.zip", "shapefiles_FARS.zip")){
    uz <- paste("unzip", file.path(localdir, dd))
    system(uz)
  }
  # move any files which are in an unnecessary "shapefiles" folder up to the top level of the localdir
  system("mv -v ~/workingdata/shapefiles/* ~/workingdata/")
}

# rename data files by month. For each month, prep time and response variables
# See prep.hex() in wazefunctions.R for details.
for(mo in do.months){
  prep.hex(file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo, "_", HEXSIZE, "_mi.RData")), month = mo)
}

# Plot to check grid IDs
CHECKPLOT = F
if(CHECKPLOT){
  grid_shp <- rgdal::readOGR(localdir, "MD_hexagons_1mi_newExtent_newGRIDID")
  
  w.g <- match(w.04$GRID_ID, grid_shp$GRID_ID)
  w.g <- w.g[!is.na(w.g)]
  gs <- grid_shp[w.g,]
  
  plot(gs, col = "red")
}

# Add FARS, AADT, HPMS, jobs
na.action = "fill0"
for(w in c("w.04", "w.05", "w.06", "w.07","w.08", "w.09")){
  append.hex(hexname = w, data.to.add = "FARS_MD_2012_2016_sum_annual", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_AADT_total_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_routes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_lodes_sum", na.action = na.action)
  append.hex(hexname = w, data.to.add = "hexagons_1mi_bg_rac_sum", na.action = na.action)
}

w.04_09 <- rbind(w.04, w.05, w.06, w.07, w.08, w.09)

avail.cores = parallel::detectCores()

if(avail.cores > 8) avail.cores = 10 # Limit usage below max if on r4.4xlarge instance

rf.inputs = list(ntree.use = avail.cores * 50, avail.cores = avail.cores, mtry = 10, maxnodes = 1000, nodesize = 100)

keyoutputs = redo_outputs = list() # to store model diagnostics

# Omit as predictors in this vector:
alwaysomit = c(grep("GRID_ID", names(w.04), value = T), "day", "hextime", "year", "weekday", 
               "uniqWazeEvents", "nWazeRowsInMatch", 
               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
               grep("EDT", names(w.04), value = T))

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

response.var = "MatchEDT_buffer_Acc"



# A: All Waze ----

# 18 Base: All Waze features from event type (but not the counts of all Waze events together)
# 19 Add FARS only
# 20 Add Weather only
# 21 Add road class, AADT only
# 22 Add jobs only
# 23 Add all together
starttime = Sys.time()

omits = c(alwaysomit,
          "wx",
          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
          grep("F_SYSTEM", names(w.04), value = T), # road class
          c("MEAN_AADT", "SUM_AADT", "SUM_miles"), # AADT
          grep("WAC", names(w.04), value = T), # Jobs workplace
          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

modelno = "18"

if(!REASSESS){
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                                omits, response.var = "MatchEDT_buffer_Acc", 
                                # thin.dat = 0.01,
                                model.no = modelno, rf.inputs = rf.inputs) 
  
  save("keyoutputs", file = paste0("Output_to_", modelno))
} else {
  
  redo_outputs[[modelno]] = reassess.rf(train.dat = w.04_09, 
                                        omits, response.var = "MatchEDT_buffer_Acc", 
                                        model.no = modelno, rf.inputs = rf.inputs) 
}



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