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
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles

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

waze.edt.hex$DayOfWeek <- as.factor(waze.edt.hex$DayOfWeek)
waze.edt.hex$hour <- as.numeric(waze.edt.hex$hour)

summary(waze.edt.hex$nMatchEDT_buffer)
summary(waze.edt.hex$nMatchEDT_buffer > 1)
table(fitdat.w$nMatchEDT_buffer[fitdat.w$nMatchEDT_buffer > 1])

# Response variable: use continuous? Only 1,000 out of 238,000 cells have > 1 EDT event matching. Consider converting to binary. Of the >1 cell, 886 are 2 events, 122 3 events, tiny number have greater.

# Going to binary:
waze.edt.hex$MatchEDT_buffer <- waze.edt.hex$nMatchEDT_buffer
waze.edt.hex$MatchEDT_buffer[waze.edt.hex$MatchEDT_buffer > 0 ] = 1 
waze.edt.hex$MatchEDT_buffer <- as.factor(waze.edt.hex$MatchEDT_buffer)

# Analysis ----
# Prepare data and formulas

fitdat.w <- waze.edt.hex
fitvars.w <- names(waze.edt.hex)[is.na(match(names(waze.edt.hex),
                                             c("GRID_ID", "day", "uniqEDTreports")))]

fitdat.w <- fitdat.w[complete.cases(fitdat.w[,fitvars.w]),]

wazeformula <- reformulate(termlabels = fitvars.w[is.na(match(fitvars.w,
                                                              "MatchEDT_buffer"))], 
                           response = "MatchEDT_buffer")

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Conditional random forest from ctree ----
# CPU intensive, but not RAM intensive. doMC should help.
showobjsize() # from wazefunctions.R


starttime <- Sys.time()

rf.w <- randomForest(wazeformula,
                     data = fitdat.w,
                     ntree=500, nodesize=5, mtry=9)

rf.w.pred <- predict(rf.w, fitdat.w[fitvars.w[is.na(match(fitvars.w,
                                                          "MatchEDT_buffer"))]])

table(fitdat.w$MatchEDT_buffer, rf.w.pred)

cf.w = partykit::cforest(wazeformula,
             data = fitdat.w[1:1000,],
             trace = T,
             ntree = 250)

cat(Sys.time() - starttime, " elapsed for Waze")
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
