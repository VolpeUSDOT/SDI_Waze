# CART and Random Forest work for Waze

# Goals: produce classification and regression trees for features of a. Waze events which predict matching with EDT events and b. EDT events which predict matching with Waze events. Then extend this to random forests, with diagnostics including k-fold cross-validation and confusion matrices.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(rpart)
library(randomForest)
# library(reprtree) # library(devtools); install_github('araastat/reprtree')
library(maptree) # for better graphing
library(party)
library(partykit)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(CORElearn)
library(ggRandomForests)
library(GGally) # for ggpairs

# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "CORElearn", "ggRandomForests", "GGally", "rgdal"), dep = T)

setwd("~/")
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} # change this to match the mapped network drive on your machine (Z: in original)
if(length(grep("sudderth", getwd())) > 0) {mappeddrive = "S:"} #this line does not work for me
mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")
codeloc <- "~/GitHub/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

setwd(wazedir)

# read utility functions, inclulding movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

# <><><><><><><><><><><><><><><><><><><><>
# Data prep ----

PREPNEW = F # Use this to skip the data prep steps, reading directly from prepared data

if(!PREPNEW) load("CART_data_April.RData")

if(PREPNEW) {
  # Read in merged Waze and EDT data 
  ew <- readRDS(file.path(wazedir, "merged.waze.edt.April_MD.rds"))
  
  # Read in full Waze and full EDT data
  load(file.path(wazedir, "2017-04_1_CrashFact_edited.RData"))
  # Waze: Comes from aggregated monthly Waze events, clipped to a 0.5 mile buffer around MD, for April 2017
  load(file.path(wazedir, "MD_buffered__2017-04.RData"))
  
  # Link file
  link <- read.csv(file.path(wazedir, "EDT_Waze_link_April_MD.csv"))
  
  # Apply matching variable to full data sets
  
  d$WazeMatch <- !is.na(match(d$uuid, unique(link[,2])))
  
  edt.april$EDTMatch <- !is.na(match(edt.april$ID, unique(link[,1])))
  
  # Get Urban Areas from UrbanArea_overlay file 
  ua <- readOGR(file.path(mappeddrive, "SDI Pilot Projects/Waze/Working Documents/Census Files"), layer = "cb_2016_us_ua10_500k")
  
  proj4string(d) <- proj4string(edt.april) <- proj4string(ua) 
  
  waze_ua_pip <- over(d, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in d. 
  edt_ua_pip <- over(edt.april, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in edt.april. 
  
  d@data <- data.frame(d@data, waze_ua_pip)
  names(d@data)[(length(d@data)-1):length(d@data)] <- c("Waze_UA_Name", "Waze_UA_Type")
  
  edt.april@data <- data.frame(edt.april@data, edt_ua_pip)
  names(edt.april@data)[(length(edt.april@data)-1):length(edt.april@data)] <- c("EDT_UA_Name", "EDT_UA_Type")
  
  edt.april$UA <- !is.na(edt.april$EDT_UA_Name)
  d$UA <- !is.na(d$Waze_UA_Name)
  
  edt.april$EDTMatch <- as.factor(edt.april$EDTMatch)
  edt.april$UA <- as.factor(edt.april$UA)
  
  d$WazeMatch <- as.factor(d$WazeMatch)
  d$UA <- as.factor(d$UA)
  d$DayofWeek <- as.factor(format(d$time, "%a"))
  d$roadType <- as.factor(d$roadType)
  
  d$HourofDay <- as.numeric(format(d$time, "%H"))
  
  # Save prepped data for faster loading from scratch
  save(file = "CART_data_April.RData",
       list = c("edt.april","d","ew","link","wazedir"))
}


# <><><><><><><><><><><><><><><><><><><><><>
# EDT data exploratory ----
# which variables in EDT data are all zeros?

allz <- apply(edt.april@data, 2, function(x) all(x == 0 | is.na(x))) 
names(edt.april)[allz]

# Atmospheric conditions
with(edt.april@data, table(EDTMatch, AtmosphericConditions)) # For all variables, more in the matching category. Leave this one out, less useful probably. Or at least collapse to Clear, Rainy, or other.

Atmospheric <- as.character(edt.april@data$AtmosphericConditions)

Atmospheric[!is.na(match(Atmospheric, c("Cloudy", "Not Reported", "Fog, Smog, Smoke", "Unknown", "Severe Crosswinds", "Snow", "Sleet or Hail", "")))] = "Other"

edt.april@data$Atmospheric = as.factor(Atmospheric)

# Light conditions
with(edt.april@data, table(EDTMatch, LightConditions)) # unmatching ones are usually Dark (lighted or not lighted). Mathching are in Daylight. Collapse this varaible to three levels, dark, daylight, and other, for easier interpretation

Light <- as.character(edt.april@data$LightConditions)

Light[!is.na(match(Light, c("Dark - Lighted", "Dark - Not Lighted", "Dark - Unknown Lighting")))] = "Dark"
Light[!is.na(match(Light, c("Unknown", "", "Dawn", "Dusk")))] = "Other"

edt.april@data$Light = as.factor(Light)

levels(edt.april@data$UA) = c("Rural", "Urban")

# Extent of damage
with(edt.april@data, table(EDTMatch, MaxExtentofDamage)) # Collapse to Disabling, Functional, Minor, other
Damage <- as.character(edt.april@data$MaxExtentofDamage)

Damage[!is.na(match(Damage, c("Minor Damage", "No Damage", "Not Reported", "Unknown", "")))] = "Other"

Damage <- sub(" Damage", "", Damage)

edt.april@data$Damage = as.factor(Damage)

fitvars <- c("EDTMatch", "DayOfWeek", "HourofDay", "Light", "Atmospheric", "Damage", "TotalFatalCount", "UA")

# Examine variables: EDT
# pdf(file.path(wazefigdir, "Pairwise_comparison_EDT_vars.pdf"), width = 15, height = 15); ggpairs(edt.april@data[fitvars]); dev.off()


# <><><><><><><><><><><><><><><><><><><><><>
# Waze data exploratory ----

with(d@data, table(WazeMatch, subtype)) # Rename for conciseness

subType <- as.character(d@data$subtype)

subType <- sub("HAZARD_", "", subType)
subType <- sub("WEATHER_", "", subType)
subType <- sub("ON_ROAD_", "", subType)
subType <- sub("JAM_", "", subType)
subType <- sub("ON_SHOULDER_", "", subType)

d@data$subType <- as.factor(subType)

with(d@data, table(WazeMatch, roadType)) # collapse these

Road <- as.character(d@data$roadType)


Road[!is.na(match(Road, c("3", "4")))] = "Freeway"
Road[!is.na(match(Road, c("1", "2", "6")))] = "Street"
Road[!is.na(match(Road, c("0")))] = "Unknown"
Road[!is.na(match(Road, c("5", "7", "8", "17", "20")))] = "Other"

d@data$Road <- as.factor(Road)

levels(d@data$UA) = c("Rural", "Urban")

fitvars.w <- c("WazeMatch", "median.reliability", "nrecord", "type", "Road", "UA", "DayofWeek", "HourofDay")

# Examine variables: EDT
# pdf(file.path(wazefigdir, "Pairwise_comparison_Waze_vars.pdf"), width = 15, height = 15); ggpairs(d@data[fitvars.w]); dev.off()


# Prepare data and formulas

fitdat <- edt.april@data
fitvars <- c("EDTMatch", "DayOfWeek", "HourofDay", "Light", "Atmospheric", "Damage", "TotalFatalCount", "UA")
fitdat <- fitdat[complete.cases(fitdat[,fitvars]),]
edtformula <- reformulate(termlabels = fitvars[2:length(fitvars)], response = 'EDTMatch')


fitdat.w <- d@data
fitvars.w <- c("WazeMatch", "median.reliability", "nrecord", "type","subType", "Road", "UA", "DayofWeek", "HourofDay")
fitdat.w <- fitdat.w[complete.cases(fitdat.w[,fitvars.w]),]
wazeformula <- reformulate(termlabels = fitvars.w[2:length(fitvars.w)], response = 'WazeMatch')

# Subset Waze for random forest ----
# If only Waze accidents, only 15,000 records with complete cases
# Only Waze accidents + jams + road closed, 196,707 records with complete cases

fitdat.w <- d@data[d@data$type != "WEATHERHAZARD",]
fitvars.w <- c("WazeMatch", "median.reliability", "nrecord","subType", "Road", "UA", "DayofWeek", "HourofDay")
fitdat.w <- fitdat.w[complete.cases(fitdat.w[,fitvars.w]),]
wazeformula <- reformulate(termlabels = fitvars.w[2:length(fitvars.w)], response = 'WazeMatch')

# <><><><><><><><><><><><><><><><><><><><>
# Start CART ----

# ctree approach from party. Specify this package with party::, otherwise the slightly different version from partykit is used.

pdf(file.path(wazefigdir, "Regression_Tree_Plots.pdf"),
    width = 18, height = 10)

ct = party::ctree(edtformula,
           data = fitdat,
           controls = party::ctree_control(maxdepth = 3))

plot(ct, main="EDT Event Matching")

ct.w = party::ctree(wazeformula,
           data = fitdat.w,
           controls = party::ctree_control(maxdepth = 10))

plot(ct.w, main="Waze Event Matching")


dev.off()
system(paste("open ", shQuote(file.path(wazefigdir, "Regression_Tree_Plots.pdf"))))


jpeg(file.path(wazefigdir, "EDT_Regression_Tree_Plot.jpeg"),
     width = 3600, height = 1800, quality = 100,
     pointsize = 8.5,
     res = 300)
plot(ct, main="EDT Event Matching")
dev.off(); system(paste("open ", shQuote(file.path(wazefigdir, "EDT_Regression_Tree_Plot.jpeg"))))


# Table of prediction errors
table(predict(ct), fitdat$EDTMatch)

# Estimated class probabilities
tr.pred = predict(ct, newdata=fitdat, type="prob")
hist(unlist(tr.pred))


# <><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Conditional random forest from ctree ----
# Will allow ordered variables for crash severity. 
# CPU intensive, but not RAM intensive. doMC should help.

starttime <- Sys.time()

# Default 500 trees

cf = party::cforest(edtformula,
           data = fitdat,
           controls = party::cforest_unbiased())#, 
#           trace = TRUE) #Error with this argument

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), " elapsed for EDT")


pt <- prettytree(cf@ensemble[[1]], names(cf@data@get("input")))
nt <- new("BinaryTree") 
nt@tree <- pt 
nt@data <- cf@data 
nt@responses <- cf@responses 

# plot(nt) # needs work

# predictions
cf.pred <- predict(cf, OOB = T)

(predtab <- table(fitdat$EDTMatch, cf.pred))

bin.mod.diagnostics(predtab) # from wazefunctions.R

# Waze forest ----
showobjsize() # from wazefunctions.R

rm(list = ls()[is.na(match(ls(), 
                           c("fitdat.w", "fitdat", "wazeformula", "edtformula", "cf", "cf.pred",
                             "wazedir", "wazefigdir", "codeloc")))])
gc() # Strong remval of anything except for these objects; use with caution. gc() clears unused memory.

source(file.path(codeloc, 'utility/wazefunctions.R')) # read functions back in

starttime <- Sys.time()

cf.w = partykit::cforest(wazeformula,
             data = fitdat.w,
#             control = partykit::ctree_control(),
             trace = T,
             ntree = 250)

cat(Sys.time() - starttime, " elapsed for Waze")
# ~ 5 min for accident type only using party

format(object.size(cf.w), units = "Mb")

# Save forest output ----
outputdir_temp <- tempdir()
outputdir_final <- wazedir


# predictions
cf.w.pred <- predict(cf.w, OOB = T) # 144 Gb vector needed for predictions, fails if using all but weather hazards.

wt <- table(fitdat.w$WazeMatch, cf.w.pred)

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



# plot(WazeMatch)
# 
# plot(cf.w, main="Waze Event Matching")


# <><><><><><><><><><><><><><><><><><><><><><><><><><><>

# Other approaches ----

# RWeka 
# Relies on Java library of machine learning tools. Architechture of R and Java have to match; both should be 64-bit. If necessary, uninstall old Java and install correct version (https://java.com/en/download/manual.jsp)

library(RWeka)


# Rpart 
fit.e.1 <- rpart(edtformula,
                 data = fitdat)
par(mar = rep(5, 4), xpd = T)
plot(fit.e.1)
text(fit.e.1, use.n=T)


# CORElearn
## Random Forests
rf.e = CoreModel(edtformula,
                 data=fitdat,
                 model="rf", selectionEstimator="MDL", minNodeWeightRF=5, rfNoTrees=100)
plot(rf.e)

# decision tree with naive Bayes in the leaves
fit.dt = CoreModel(edtformula,
                   data=fitdat,model="tree", modelType=4)
plot(fit.dt, fitdat)

plot(fit.dt, fitdat, graphType="attrEval")

rp <- getRpartModel(fit.dt, fitdat)
plot(rp, branch=0.5, minbranch=5, compress=TRUE)
text(rp, use.n=T)

####
fitdat$DayOfWeek <- as.factor(fitdat$DayOfWeek)

fit.rf = randomForest(UA ~ roadType + DayOfWeek + HourofDay + type + , 
                      data = fitdat)
print(fit.rf)
importance(fit.rf)
plot(fit.rf)
plot( importance(fit.rf), lty=2, pch=16)
lines(importance(fit.rf))
imp = importance(fit.rf)
impvar = rownames(imp)[order(imp[, 1], decreasing=TRUE)]
op = par(mfrow=c(1, 3))

for (i in seq_along(impvar)) {
  partialPlot(fit.rf, raw, impvar[i], xlab=impvar[i],
              main=paste("Partial Dependence on", impvar[i]),
              ylim=c(0, 1))
}


reprtree:::plot.getTree(fit.rf, depth = 5)

# This shows the likelihood that a matching event will be Urban


# Let's look at Waze events that match vs don't match

ew$waze.Day <- as.factor(format(ew$time, "%a"))
ew$waze.Hour <- as.numeric(format(ew$time, "%H"))

fitdat <- ew[!is.na(ew$uuid.waze),]
fitvars <- c("match", "roadType", "type", "time", "UA", "waze.Day", "waze.Hour")
fitdat <- fitdat[complete.cases(ew[fitvars]),]

fitdat$match <- as.factor(fitdat$match)
fitdat$roadType <- as.factor(fitdat$roadType)

fit.rf.w <- randomForest(match ~ roadType + type + waze.Day + waze.Hour + UA + reliability,
              data = fitdat[sample(1:nrow(fitdat), 1e5),])

print(fit.rf.w)
importance(fit.rf.w)
plot(fit.rf.w)
plot( importance(fit.rf.w), lty=2, pch=16)
lines(importance(fit.rf.w))
imp = importance(fit.rf.w)
impvar = rownames(imp)[order(imp[, 1], decreasing=TRUE)]


reprtree:::plot.getTree(fit.rf.w, depth = 4)

# faster moving estimates of crash estiamtes that is reliable in space and time. Share reg tree approach on matched events. 
# overall approach
# visual of the analysis steps. 

# EDT vars to use: extent of damage, highest injury level, light condition, time.., manner of collision, n vehicles involved, roadway traffic identifier (maybe), road intersection (maybe), atmospheric



