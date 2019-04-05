# Analysis of crashes on Bellevue road network segmetns.
# Analysis approaches:
# - Simple linear regression for count of crashes
# - Simple logistic regression for presence of crashes
# - Zero-inflated negative binomial
# - Hierarchical by road class..
# - Also do random forests / xgboost? Perhaps, but likely not as useful in this context since we want to interpret coefficients of individual factors
# In each of these, test different hypotheses with inclusion of time, FARS, LEHD, weather, and other variables.

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

source(file.path(codeloc, 'utility/get_packages.R'))

library(tidyverse)
library(ggplot2)
library(GGally)
library(dplyr)
library(corrplot)
library(Amelia)
library(mlbench)
library(xts)

# mapping related packages
library(maps) # for mapping base layers
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(rgeos) # gintersection
library(ggmap)
library(spatstat)

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
output.loc <- file.path(data.loc, "Segments")

setwd(data.loc)

# Check if prepared data are available; if not, run Segment Aggregation.
Waze_Prepared_Data = dir(output.loc)[grep("^Bellevue_Waze_Segments_", dir(output.loc))]

if(length(grep(Waze_Prepared_Data, dir(output.loc))) == 0){
  stop(paste("No Bellevue segment data available in", output.loc, "\n Run Segment_Aggregation_Bell.R or check network connection"))
}  else {
  load(file.path(output.loc, Waze_Prepared_Data))
}


# <><><><><><><><><><><><><><><><><><><><><><><><>
# Variables organization: Start from prepared data for 1 hour window ----
table(w.all$uniqueCrashreports) # only 3 segment have more than 1 crash, logistic regression is more appropriate for this data.
# 0     1     2 
# 59870  1364     3 

# Omit or include predictors in this vector:
alwaysomit = c(grep("RDSEG_ID", names(w.all), value = T), "year", "day", "segtime", "weekday", 
               grep("Crash", names(w.all), value = T),
               "OBJECTID")

alert_types = c("nWazeAccident", "nWazeJam", "nWazeRoadClosed", "nWazeWeatherOrHazard")

alert_subtypes = c("nHazardOnRoad", "nHazardOnShoulder" ,"nHazardWeather", "nWazeAccidentMajor", "nWazeAccidentMinor", "nWazeHazardCarStoppedRoad", "nWazeHazardCarStoppedShoulder", "nWazeHazardConstruction", "nWazeHazardObjectOnRoad", "nWazeHazardPotholeOnRoad", "nWazeHazardRoadKillOnRoad", "nWazeJamModerate", "nWazeJamHeavy" ,"nWazeJamStandStill",  "nWazeWeatherFlood", "nWazeWeatherFog", "nWazeHazardIceRoad")

waze_rd_type = grep("WazeRT", names(w.all), value = T)[-c(1,2,6)] # counts of events happened at that segment at each hour. All zero for road type 0, 3, 4, thus removing them

waze_dir_travel = grep("MagVar", names(w.all), value = T)[-2] # nMagVar30to60 is all zero, removing it from the variable list

weather_var = c('PRCP', 'TMIN', 'TMAX', 'SNOW')

other_var = c("nBikes", "nFARS")

time_var = c("hour", "mo", "weekday", "day") # time variable can be used as indicator or to aggregate the temporal resolution.

seg_var = c("Shape_STLe", "SpeedLimit", "ArterialCl"
            # , "FunctionCl"
            ) # "ArterialCl" has no missing values. There are 6 rows with missing values in "FunctionCl", therefore if we use in the model, we will lose these rows.

# Create a list to store the indicators
indicator.var.list <- list("seg_var" = seg_var, "other_var" = other_var, "weather_var" = weather_var, "alert_types" = alert_types, "alert_subtypes" = alert_subtypes, "waze_rd_type" = waze_rd_type, "waze_dir_travel" = waze_dir_travel)

# A list of Response variables
response.var.list <- c(
                  "uniqueCrashreports", # number of crashes at each segment at every hour of day
                  "biCrash",            # presence and absence of crash at each segment at every hour of day
                  "nCrashes"            # total crashes at each segment of entire year 2018
                  )
# ncrash.1yr.excludeInt  # have not created yet, should be "nCrashes" - "Crash_End1" - "Crash_End2"
# ncrash.4hr = NA # if we use 4 hour window, have not created this variable yet

# Correlation & ggpairs ----
correlations <- cor(w.all[, c(response.var.list, "Shape_STLe")])
corrplot(correlations, method="circle", type = "upper", 
         diag = F,
         tl.col = "black"
         # , tl.srt = 45
         # , main = "Correlation Plots"
)

# ggpairs to look at scatter, boxplot, and density plots, as well as correlation, tried to save as pdf, too slow to open.
# alternative: use boxplots for categorical variables

for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  f <- paste0(data.loc,'/Model_visualizations/ggpairs_', names(indicator.var.list)[i],".png")
  
  # if the file exists, then don't regenerate as it takes a few minutes for each plots
  if (!file.exists(f)) {
      png(file = f,  
          width = ceiling(n/2)*2, # or use 12 for all of them 
          height = ceiling(n/2)*2,
          units = 'in', 
          res = 300)
      g <- ggpairs(w.all[, c(response.var.list, indicator.var.list[[i]])])
      
      print(g)
      
      dev.off()
  }
}

# check missing values and all zero columns ----
# Checked all interested predictors listed above, they are good!
# missmap(w.all[, alert_types], col = c("blue", "red"), legend = FALSE)

for (i in 1:length(indicator.var.list)) {
  
  n <- length(c(response.var.list, indicator.var.list[[i]]))
  
  f <- paste0(data.loc,'/Model_visualizations/missmap_', names(indicator.var.list)[i],".png")
  
  # if the file exists, then don't regenerate as it takes a few minutes for each plot
  if (!file.exists(f)) {
    png(file = f,  
        width = ceiling(n/2)*2, # or use 12 for all of them 
        height = ceiling(n/2)*2,
        units = 'in', 
        res = 300)
    missmap(w.all[, indicator.var.list[[i]]], col = c("blue", "red"), legend = FALSE)
    
    dev.off()
  }
}

# if any other columns are all zeros
all_var <- vector()
for (i in 1:length(indicator.var.list)){
  all_var <- c(all_var, indicator.var.list[[i]])
}

lapply(w.all[, all_var], function(x) all(x == 0)) # all variables are clean now. None of them are all-zero column.


# Check time variables ----
stopifnot(length(unique(w.all$day)) == 365) # 365
stopifnot(length(unique(w.all$mo)) == 12)   # 12
stopifnot(length(unique(w.all$hour)) == 24) # 24

day.hour <- data.frame("time_hr" = seq(from = as.POSIXct("2018-01-01 0:00", tz = 'America/Los_Angeles'), 
                  to = as.POSIXct("2018-12-31 0:00", tz = 'America/Los_Angeles'),
                  by = "hour")
                  )
# day.hour <- as.character(day.hour)
# Create time_hr in w.all
w.all = w.all %>%
  mutate(time_hr = as.POSIXct(segtime, '%Y-%j %H', tz = 'America/Los_Angeles'))

w.sub <- w.all[, c("time_hr", "RDSEG_ID", "uniqueCrashreports", "uniqueWazeEvents", "nWazeAccident", "nWazeJam")]

w.sub <- day.hour %>% left_join(w.sub, by = c("time_hr")) %>% 
  mutate_if(is.numeric, coalesce, 0) %>% 
  mutate(
    year = format(time_hr, "%Y"),
    day = format(time_hr, "%j"),
    hour = format(time_hr, "%H")
    )

is.unsorted(w.sub$time_hr) # the data now is sorted by time.

# Aggregate by RDSEG_ID, year, day, and hour
w.sub_seg <- w.sub %>% 
  group_by(time_hr, year, day, hour) %>% 
  summarize(
    uniqueCrashreports = sum(uniqueCrashreports),
    uniqueWazeEvents = sum(uniqueWazeEvents),
    nWazeAccident = sum(nWazeAccident),
    nWazeJam =  sum(nWazeJam)
    )

is.unsorted(w.sub_seg$time_hr) # still in the order

# Convert to timeseries

df2 <- xts(x = w.sub_seg[!names(w.sub_seg) %in% 'time_hr'], order.by = w.sub_seg$time_hr)

f <- paste0(data.loc,'/Model_visualizations/time_series.png')

png(f, width = 6, height = 8, units = 'in', res = 300)
par(mfrow=c(3, 2))

# by day hour
plot(w.sub_seg$uniqueCrashreports, type = 'l', main = "By day hour", xlab = "time_hr", ylab = "Number of Crashes")


# by day

# by hour
w.sub_seg_hr <- w.sub_seg %>% group_by(time_hr, year, day, hour) %>% summarize(
  uniqueCrashreports = sum(uniqueCrashreports),
  uniqueWazeEvents = sum(uniqueWazeEvents),
  nWazeAccident = sum(nWazeAccident),
  nWazeJam =  sum(nWazeJam)
)
plot(w.sub_seg_hr$uniqueCrashreports, type = 'l', main = "By hour", xlab = "Hour", ylab = "Number of Crashes")


# by month


dev.off()



# <><><><><><><><><><><><><><><><><><><><><><><><>
# Start Modelings ----
# Start simple
# 01 Base: Road network (seg_var)
# 02: Add FARS only ("nFARS")
# 03: Add Weather only (weather_var)
# 04: Add Waze features from event type (but not the counts of all Waze events together) alert_types, alert_subtypes
# 05: Add all other Waze features (waze_rd_type, waze_dir_travel)


starttime = Sys.time()

# Make a list of variables to omit from the predictors. To add variables, comment out the corresponding line.
# month (mo) and hour are categorical variables. 

includes = c(
              waze_rd_type,    # road types from Waze
              waze_dir_travel, # direction of travel
              alert_types,     # counts of waze events by alert types
              alert_subtypes,  # counts of waze events by sub-alert types
              weather_var,     # Weather variables
              "nFARS",         # FARS variables
              "nBikes",        # bike/ped conflict counts at segment level (no hour)
              "hour",          # hour
              seg_var
          )

modelno = "08"

response.var <- response.var.list[2] # binary data, biCrash

# Simple 

( predvars = names(w.all)[names(w.all) %in% includes] )

( use.formula = as.formula(paste(response.var, "~", 
                                 paste(predvars, collapse = "+"))) )

assign(paste0('m', modelno),
       # lm(use.formula, data = w.all) # Linear model
       glm(use.formula, data = w.all, family = "binomial") # logistic regression
       )

# Summarize

summary(get(paste0('m', modelno)))

# summary.aov(get(paste0('m', modelno)))

# Model output visuals ----
# m08 logistic regression using all variables
w.all$m08_fit <- predict(m08, type = "response")

out.put <- w.all[, c("RDSEG_ID", "segtime", "m08_fit", "uniqueCrashreports")]
out.put <- out.put %>% group_by(RDSEG_ID) %>% summarize(m08_fit = sum(m08_fit),
                                                        uniqueCrashreports = sum(uniqueCrashreports))

# Dan: I suggest we split out the visualization into separate scripts for simplicity, and have the analysis script end by saving a .RData of all the model outputs only.
# Jessie: good idea, this is just a temporary space for an quick examination of the visuals. if we think R could make some good visuals, we'll use a separate code to store these.

roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
names(roadnettb_snapped)

roadnettb_snapped@data <- roadnettb_snapped@data %>% left_join(out.put, by = c("RDSEG_ID")) # adding model output to the spatial data frame

plot(roadnettb_snapped, col = roadnettb_snapped@data$m08_fit)
# legend("bottomright") # To do add legend, right now no idea of the color.

# ggmaps----
# Get a map for plotting, need to define bounding box (bbox) defined in decimal degree lat long. This is what get_stamenmaps requires. Can also make a buffer around the road network to ensure we have a large enough area to cover the whole city. Here just using road network.
state = "WA"

model_out_road = spTransform(roadnettb_snapped, CRS =  CRS("+proj=longlat +datum=WGS84"))

bbox.bell = bbox(model_out_road) # Can also use extend.range to get a larger bouding box

if(length(grep(paste0(state, "_Bellevue_Basemaps"), dir(file.path(output.loc)))) == 0){
  map_terrain_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'terrain', zoom = 14)
  map_toner_hybrid_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'toner-hybrid', zoom = 14)
  map_toner_14 <- get_stamenmap(bbox = as.vector(bbox.bell), maptype = 'toner', zoom = 14)
  save(list = c("map_terrain_14", "map_toner_hybrid_14", "map_toner_14"),
       file = file.path(output.loc, paste0(state, "_Bellevue_Basemaps.RData")))
} else { load(file.path(output.loc, paste0(state, "_Bellevue_Basemaps.RData"))) }


# map <- qmap('Bellevue', zoom = 12, maptype = 'hybrid')

# To use geom_path, we need to extract the lat-long start and end for every segment.
# this is probably not worth it, let's just export and plot in Tableau / ArcGIS
# Jessie: make sense. I'll export the layer for mapping in ArcGIS.
# https://stackoverflow.com/questions/32413190/how-to-plot-spatiallinesdataframe-feature-map-over-google-maps

NOTRUN = F

if(NOTRUN){

ggmap(map_toner_hybrid_14, extent = 'device') + 
  geom_path(data = model_out_road,
            aes(mapping = aes(x = long, y = lat, group = RDSEG_ID)), 
            size=2)
}
# 4/3/2019 Todos : 1. scale the numeric columns, and re-run logistic regressions. 2. Lasso; 3. logistic regression - present the coefficients using OR; 4. logistic regression with mixed effects; 5. double check the hour column; 6. aggregate data into multi-hour window; 7. Do a model with just the one hour of data. 8. incoporate day of week, month, hour of day as the temporal indicators; 9. XGBoost
# x <- w.all %>% group_by(hour) %>% summarize(sumCrash = sum(uniqueCrashreports))
# use as reference: https://medium.com/geoai/using-machine-learning-to-predict-car-accident-risk-4d92c91a7d57
# https://gduer.github.io/Collision-Prediction-in-Louisville-KY/#5_model_selection

# once we aggregate to a 4 hour window or a day, the counts might change, then we will need a zero-inflated NB model.

# TODO: extract model diagnostics, save in a list..
# create a summary table with diagnostics for all the linear model ----
# Get the list of linear models
ClassFilter <- function(x) inherits(get(x), 'lm' ) & !inherits(get(x), 'glm') # excluding other potential classes that contains "lm" as the keywords.
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(output.loc, "Bell_linear_model_summary_list.Rdata")
# out.name <- file.path(data.loc, 'Segments', "Bell_linear_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(codeloc, "utility/Model_Summary().R")
    linear_model_summary_list <- linear_model_summary(model_list, out.name)
  }
# save(list = c("linear_model_summary_list"), file = file.path(data.loc, 'Segments', "Bell_linear_model_summary_list.Rdata"))

M <- linear_model_summary_list$M
model_summary  <- linear_model_summary_list$model_summary
model_compare <- linear_model_summary_list$model_compare


# create a summary table with diagnostics for all the logistic model ----
# Get the list of logistic models
ClassFilter <- function(x) inherits(get(x), 'glm')
row.name <- Filter( ClassFilter, ls() )
model_list <- lapply( row.name, function(x) get(x) )

out.name <- file.path(data.loc, 'Segments', "Bell_logistic_model_summary_list.Rdata")

if(file.exists(out.name)){
  load(out.name)} else {
    source(file.path(codeloc, "/WA/utility/Model_Summary().R"))
    logistic_model_summary_list <- logistic_model_summary(model_list, out.name)
  }
# save(list = c("logistic_model_summary_list"), file = file.path(data.loc, 'Segments', "Bell_logistic_model_summary_list.Rdata"))

M <- logistic_model_summary_list$M
model_summary  <- logistic_model_summary_list$model_summary
model_compare <- logistic_model_summary_list$model_compare


timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to model", modelno)

