# Random Forest work for Waze

# Goals: produce random forest analysis for features of Waze accidents and other event types that predict matching with EDT reports 

# Running on gridded data

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(rpart)
library(randomForest)
library(maptree) # for better graphing
library(party)
library(partykit)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel


# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal"), dep = T)

setwd("~/")
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} 
if(length(grep("EASdocs", getwd())) > 0) {mappeddrive = "S:"} 
# mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")
codeloc <- "~/git/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

setwd(wazedir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

load("WazeEdtHex_Beta.RData")

waze.edt.hex <- wazeTime.edt.hex

waze.edt.hex$DayOfWeek <- as.factor(waze.edt.hex$DayOfWeek)
waze.edt.hex$hour <- as.numeric(waze.edt.hex$hour)


# Response variable: use continuous? Only 1,600 out of 310,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater. 15,000 have 1.
summary(waze.edt.hex$nMatchEDT_buffer)
summary(waze.edt.hex$nMatchEDT_buffer > 1)
table(waze.edt.hex$nMatchEDT_buffer[waze.edt.hex$nMatchEDT_buffer > 1])

# Going to binary:
waze.edt.hex$MatchEDT_buffer <- waze.edt.hex$nMatchEDT_buffer
waze.edt.hex$MatchEDT_buffer[waze.edt.hex$MatchEDT_buffer > 0] = 1 
waze.edt.hex$MatchEDT_buffer <- as.factor(waze.edt.hex$MatchEDT_buffer)

# Analysis ----
# Prepare data and formulas
fitdat.w <- waze.edt.hex

# Variables to test. Use Waze only predictors, and omit grid ID and day as predictors as well

fitvars.w <- names(waze.edt.hex)[is.na(match(names(waze.edt.hex),
                                             c("GRID_ID", "day", # place variables to omit as predictors in this vector 
                                               "nMatchWaze_buffer", "nNoMatchWaze_buffer",
                                               grep("EDT", names(waze.edt.hex), value = T)
                                               )))]

fitdat.w <- fitdat.w[complete.cases(fitdat.w[,fitvars.w]),]

wazeformula <- reformulate(termlabels = fitvars.w[is.na(match(fitvars.w,
                                                              "MatchEDT_buffer"))], 
                           response = "MatchEDT_buffer")

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Random forest sequential ----
# CPU intensive, but not RAM intensive. doMC should help.
showobjsize() # from wazefunctions.R

(avail.cores <- parallel::detectCores()) # 4 on local
ntree.use = avail.cores * 100 

trainrows <- sort(sample(1:nrow(fitdat.w), size = nrow(fitdat.w)*.7, replace = F))
testrows <- (1:nrow(fitdat.w))[!1:nrow(fitdat.w) %in% trainrows]

# length(testrows) + length(trainrows) == nrow(fitdat.w)

system.time(rf.w <- randomForest(wazeformula,
                     data = fitdat.w[trainrows,],
                     ntree = ntree.use,
                     nodesize = 5,
                     mtry = 9)
            )

# predict on test values
system.time(rf.w.pred <- predict(rf.w, fitdat.w[testrows, fitvars.w]))

(predtab <- table(fitdat.w$MatchEDT_buffer[testrows], rf.w.pred))
bin.mod.diagnostics(predtab)

# rf.w.pred
#       0     1
# 0 67746   369
# 1  2850   690
# accuracy            0.9551
# precision           0.1949
# recall              0.6516
# false.positive.rate 0.0404

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Random forest parallel ----
cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# On small job, user time faster but system time greater. On a large job, almost exactly ncore X faster!

system.time(rf.p <- foreach(ntree = c(ntree.use/avail.cores, avail.cores),
                .combine = combine,
                .packages = "randomForest") %dopar%
          randomForest(wazeformula,
               data = fitdat.w[trainrows,],
               ntree = ntree,
               nodesize = 5,
               mtry = 9)
  )


# prediction not as simple to parallelize... but very fast anyways
# foreach(ntree = c(ntree.use/avail.cores, avail.cores),
#         .combine = combine,
#         .packages = "randomForest",
#         #suba = isplitRows(a, chunkSize=10) # check split rows in iterators?
#         ) %dopar% {
#   predict(rf.p, newdata = fitdat.w[fitvars.w])
# }

system.time(rf.p.pred <- predict(rf.p, fitdat.w[testrows,fitvars.w]))

(predtab <- table(fitdat.w$MatchEDT_buffer[testrows], rf.p.pred))
bin.mod.diagnostics(predtab)

# Nearly identical predictions and diagnostics. 
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

stopCluster(cl) # stop the cluster when done


# If only Waze accident report
# 1. Predict to nMatchEDT_buffer_acc
# 2. Subset to only nWazeAccident > 1 ? look at this. 

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
