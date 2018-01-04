# Classification of EDT-level police accidents by elastic net regression
# Started January 2018

library(CORElearn)
library(glmnet)
library(biglasso)
# install.packages(c("CORElearn", "glmenet", "biglasso"), dep = T)


if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} # change this to match the mapped network drive on your machine (Z: in original)
if(length(grep("sudderth", getwd())) > 0) {mappeddrive = "S:"} 

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")

setwd(wazedir)

# <><><><><><><><><><><><><><><><><><><><>
# Data prep ----
# Use prepped data from CART_Waze.R, which makes dataframes for EDT and Waze which have columns for EDTMatch and WazeMatch, giving the T/F vector for matching of at least one Waze event or at least one EDT event, respectively. These dataframes also include Urban Area as UA, DayofWeek, and HourofDay.

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

# <><><><><><><><><><><><><><><><><><><><>
# Lasso ----

# Goal: create a vector of predicted EDT-level events from the April Waze data. Assess model goodness-of-fit. Develop a graphical output, using Waze features as predictors and WazeMatch as response. If this goes well, include EDT severity to make an ordered output instead of binary T/F.



