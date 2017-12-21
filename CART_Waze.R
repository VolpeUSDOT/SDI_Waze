# CART start for Waze

# Goals: produce example (at first) classification and regression trees for features of a. Waze events which predict matching with EDT events and b. EDT events which predict matching with Waze events.

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(rpart)
library(randomForest)
# library(reprtree) # library(devtools); install_github('araastat/reprtree')
library(maptree) # for better graphing
library(party)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(CORElearn)
library(ggRandomForests)
library(GGally) # for ggpairs

# install.packages(c("rpart", "maptree", "party", "CORElearn", "ggRandomForests"), dep = T)


if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} # change this to match the mapped network drive on your machine (Z: in original)
if(length(grep("sudderth", getwd())) > 0) {mappeddrive = "S:"} 

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")


setwd(wazedir)

# <><><><><><><><><><><><><><><><><><><><>
# Data prep ----

PREPNEW = F

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
pdf(file.path(wazefigdir, "Pairwise_comparison_EDT_vars.pdf"), width = 15, height = 15); ggpairs(edt.april@data[fitvars]); dev.off()


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
pdf(file.path(wazefigdir, "Pairwise_comparison_Waze_vars.pdf"), width = 15, height = 15); ggpairs(d@data[fitvars.w]); dev.off()


# <><><><><><><><><><><><><><><><><><><><>
# Start CART ----

fitdat <- edt.april@data
fitvars <- c("EDTMatch", "DayOfWeek", "HourofDay", "Light", "Atmospheric", "Damage", "TotalFatalCount", "UA")
fitdat <- fitdat[complete.cases(fitdat[,fitvars]),]
edtformula <- reformulate(termlabels = fitvars[2:length(fitvars)], response = 'EDTMatch')


fitdat.w <- d@data
fitvars.w <- c("WazeMatch", "median.reliability", "nrecord", "type","subType", "Road", "UA", "DayofWeek", "HourofDay")
fitdat.w <- fitdat.w[complete.cases(fitdat.w[,fitvars.w]),]
wazeformula <- reformulate(termlabels = fitvars.w[2:length(fitvars.w)], response = 'WazeMatch')


# ctree approach from party

pdf("Regression_Tree_Plots.pdf",
    width = 18, height = 10)

ct = ctree(edtformula,
           data = fitdat,
           controls = ctree_control(maxdepth = 3))

plot(ct, main="EDT Event Matching")

ct.w = ctree(wazeformula,
           data = fitdat.w,
           controls = ctree_control(maxdepth = 10))

plot(ct.w, main="Waze Event Matching")


dev.off()
system("open Regression_Tree_Plots.pdf")


jpeg("EDT_Regression_Tree_Plot.jpeg",
     width = 3600, height = 1800, quality = 100,
     pointsize = 8.5,
     res = 300)
plot(ct, main="EDT Event Matching")
dev.off(); system("open EDT_Regression_Tree_Plot.jpeg")


# Table of prediction errors
table(predict(ct), fitdat$EDTMatch)

# Estimated class probabilities
tr.pred = predict(ct, newdata=fitdat, type="prob")
hist(unlist(tr.pred))

# <><><><><><><><><><><><><><><><><><><><><><><><><><><>

# Other approaches ----

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
# make this by noon Monday, for dry run on Monday afternoon.

# EDT vars to use: extent of damage, highest injury level, light condition, time.., manner of collision, n vehicles involved, roadway traffic identifier (maybe), road intersection (maybe), atmospheric



