# Parallelized random forests notes
# To compare possible implementations on ATA server. Ordered in priority of testing

# Test data: run first section of RandomForest_WazeGrid.R, up to section header "# Random forest sequential ----"
# Test push from SDC


# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# randomForest + foreach
# This uses the most widely-used implementation of random forests, along with the foreach pacakges. Not difficult to implement, and this should be the first option until shown to be not useful.



# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# Caret
# Classification and regression in parallel
install.packages("caret", dependencies = c("Depends", "Suggests"))


# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# H2O
# Potentially good for us, because implement a variety of machine learning task in a parallel way, including glm, random forest, and neural network. Large package.
# Open source math engine for big data, see h20.ai

# install.packages("h2o", dep = T)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# XGBoost
# Variables must be numeric
# install.packages('xgboost', dependencies = T)

library(xgboost)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# Ranger
# install.packages('ranger', dependencies = T)

library(xgboost)

# <<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>><<>>
# ParallelForest




# Older packages
# Archived package bigrf, no longer on CRAN
# install.packages('bigrf')




# Scratch ----

# Comparison of sequential and parallel: identical predictions and diagnostics. 
# Accuracy: sum of true positives and true negatives, as a portion of total sample.
# Precision: True positives as a portion of all predictions
# Recall: true postives as a portion of all observed positives
# False positive rate: False positives a portion of all observed negatives

# rf.p.pred
# Observed as columns, predicted as rows.
#       0     1
# 0 67740   375
# 1  2846   694

# accuracy            0.9550
# precision           0.1960
# recall              0.6492
# false.positive.rate 0.0403




# # <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# # Random forest sequential ----
# # CPU intensive, but not RAM intensive. doMC should help.
# showobjsize() # from wazefunctions.R
# 
# 
# 
# system.time(rf.w <- randomForest(wazeformula,
#                                  data = fitdat.w[trainrows,],
#                                  ntree = ntree.use,
#                                  nodesize = 5,
#                                  mtry = 9)
# )
# 
# # predict on test values
# system.time(rf.w.pred <- predict(rf.w, fitdat.w[testrows, fitvars.w]))
# 
# (predtab <- table(fitdat.w$MatchEDT_buffer[testrows], rf.w.pred))
# bin.mod.diagnostics(predtab)
# 
# # rf.w.pred
# #       0     1
# # 0 67746   369
# # 1  2850   690
# # accuracy            0.9551
# # precision           0.1949
# # recall              0.6516
# # false.positive.rate 0.0404

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Conditional random forest from ctree ----

# cf.w = partykit::cforest(wazeformula,
#              data = fitdat.w[1:1000,],
#              trace = T,
#              ntree = 250)

# cat(Sys.time() - starttime, " elapsed for Waze")
# ~ 5 min for accident type only using party

format(object.size(cf.w), units = "Mb")

# Save forest output ----
outputdir_temp <- tempdir()
outputdir_final <- wazedir


# predictions
cf.w.pred <- predict(cf.w, OOB = T) # 144 Gb vector needed for predictions,


wt <- table(as.factor(fitdat.w$nMatchEDT_buffer[1:1000]), cf.w.pred)

knitr::kable(wt)

bin.mod.diagnostics(wt) 

# With party:
#       cf.w.pred
#       FALSE TRUE
# FALSE  8170 1008
# TRUE   4358 1603

# With partykit: same overall misclassification rate, but more predicted positives overall
# |      | FALSE| TRUE|
#   |:-----|-----:|----:|
#   |FALSE |  7876| 1302|
#   |TRUE  |  4075| 1886|

save(list = c('cf.w', 'cf', 'cf.pred', 'cf.w.pred'), 
     file = file.path(outputdir_temp, 
                      paste0("Random_Forest_output_", Sys.Date(), ".RData")
     )
)

filelist <- dir(outputdir_temp)[grep("RData$", dir(outputdir_temp))]
movefiles(filelist, outputdir_temp, outputdir_final)



# Clear unused objects; only named objects retained
rm(list = ls()[is.na(match(ls(), 
                           c("fitdat.w", "fitdat", "wazeformula", "cf", "cf.pred",
                             "wazedir", "wazefigdir", "codeloc")))])
gc() # Strong remval of anything except for these objects; use with caution. gc() clears unused memory.

source(file.path(codeloc, 'utility/wazefunctions.R')) # read functions back in

