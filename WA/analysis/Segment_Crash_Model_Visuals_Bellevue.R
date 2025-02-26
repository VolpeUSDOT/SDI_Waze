# Code for visualize output of Bellevue models

# Setup ---- 
rm(list=ls()) # Start fresh

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')), # grep() does not produce a logical outcome (T/F), it gives the positive where there is a match, or no outcome if there is no match. grepl() is what we need here.
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze") # Jessie's codeloc is ~/GitHub/SDI_Waze

source(file.path(codeloc, 'utility/get_packages.R'))

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
seg.loc <- file.path(data.loc, "Segments")
output.loc <- file.path(data.loc, "Model_output")
visual.loc <- file.path(data.loc, "Model_visualizations")

setwd(data.loc)

# Prepare XGBoost summary table for Tableau ---- 
model_type = "XGB_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))
load(out.name)
for (i in 1:length(row.name)) {
  assign(row.name[i], XGB_models[[i]])
}

model_type = "RF_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))
load(out.name)
for (i in 1:length(row.name)) {
  assign(row.name[i], RF_models[[i]])
}

model_type = "Poisson_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))
load(out.name)
for (i in 1:length(row.name)) {
  assign(row.name[i], Poisson_models[[i]])
}

ArterialCl <- PredSet$ArterialCl
wkend <- PredSet$wkend
grp_hr <- PredSet$grp_hr
PredSet_xgb <- data.frame(PredSet[, includes_xgb], model.matrix(~ ArterialCl + 0), model.matrix(~ wkend + 0), model.matrix(~ grp_hr + 0))

response.var <- response.var.list[1]
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
pred_pred <- predict(m15.xgb.art.wkend.20rd, dpred$data)
cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)) # 44.31%

response.var <- response.var.list[4]
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
pred_pred_weighted <- predict(m15.xgb.art.wkend.25rd.weighted, dpred$data)
cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred_weighted )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)) # 28.75%

response.var <- response.var.list[4]
dpred <- list("data" = as.matrix(PredSet_xgb), "label" = PredSet[,response.var])
pred_pred_weighted <- predict(m15.xgb.art.wkend.25rd.weighted.v2, dpred$data)
cat("% Var explained: \n", 100 * (1-sum(( PredSet[,response.var] - pred_pred_weighted )^2) /
                                    sum(( PredSet[,response.var] - mean(PredSet[,response.var]))^2)
)) # 55.71285%

rf_Pred = predict(m15.rf.art.wkend, PredSet)
Poi_Pred = predict.glm(m15.poi.art.wkend, PredSet, type = "response")
range(Poi_Pred) 
# the message is mostly due to too many predictors in the formula of glm for the data of model 15, maybe we can use model 14 instead.
# POisson prediction Warning message:
#   In predict.lm(object, newdata, se.fit, scale = 1, type = if (type ==  :
#                                                                prediction from a rank-deficient fit may be misleading

# data with XGBoost model predictions
out <- cbind(PredSet, "Poi_Pred" = Poi_Pred, "Rf_Pred" = rf_Pred, "Xgb_Pred" = pred_pred, "Xgb_Pred_Weighted" = pred_pred_weighted)
write.csv(out, file.path(data.loc, 'Model_output', paste0("Bell_",model_type,"_pred.csv")), row.names = F)

# Visualizations ----
# Histogram
boxplot(out$Xgb_Pred ~ out$uniqueCrashreports) # observed vs predicted


# Check if prepared data are available; if not, run Segment Aggregation.
Waze_Prepared_Data = dir(seg.loc)[grep("^Bellevue_Waze_Segments_", dir(seg.loc))]

if(length(grep(Waze_Prepared_Data, dir(seg.loc))) == 0){
  stop(paste("No Bellevue segment data available in", seg.loc, "\n Run Segment_Aggregation_Bell.R or check network connection"))
}  else {
  load(file.path(seg.loc, Waze_Prepared_Data))
}

# Model fit value extraction ----
# load models
model_type = "logistic_models"
out.name <- file.path(data.loc, 'Model_output', paste0("Bell_",model_type,".Rdata"))
stopifnot(file.exists(out.name))

if(file.exists(out.name)){load(out.name)}

# m08 logistic regression using all variables
models = c("00", "01", "02", "03", "04", "05", "06", "07", "08")
w.all[, paste0('m', models, "fit")] <- NA

for (i in 1:9){
  
  m <- assign(paste0('m', models[i]), logistic_models[[i]])
  w.all[, paste0('m', models[i], "fit")] <- predict(m, type = "response")
  
}

var_include <- c("RDSEG_ID", "hour", "uniqueCrashreports", paste0('m', models, "fit"))

out.put.1hr <- w.all[, var_include] %>% 
       group_by(RDSEG_ID, hour) %>% 
  summarize(
    m00fit = sum(m00fit),
    m01fit = sum(m01fit),
    m02fit = sum(m02fit),
    m03fit = sum(m03fit),
    m04fit = sum(m04fit),
    m05fit = sum(m05fit),
    m05fit = sum(m05fit),
    m05fit = sum(m05fit),
    m08fit = sum(m08fit),
    ncrash = sum(uniqueCrashreports)
    )

var_include <- c("RDSEG_ID", "grp_4hr", "uniqueCrashreports", paste0('m', models, "fit"))

out.put.4hr <- w.all[, var_include] %>% 
  group_by(RDSEG_ID, grp_4hr) %>% 
  summarize(
    m00fit = sum(m00fit),
    m01fit = sum(m01fit),
    m02fit = sum(m02fit),
    m03fit = sum(m03fit),
    m04fit = sum(m04fit),
    m05fit = sum(m05fit),
    m05fit = sum(m05fit),
    m05fit = sum(m05fit),
    m08fit = sum(m08fit),
    ncrash = sum(uniqueCrashreports)
  )

out.put.1hr.wide <- reshape(as.data.frame(out.put.1hr), direction = "wide", idvar = "RDSEG_ID", timevar = "hour") 
out.put.1hr.wide[is.na(out.put.1hr.wide)]=0

out.put.4hr.wide <- reshape(as.data.frame(out.put.4hr), direction = "wide", idvar = "RDSEG_ID", timevar = "hour") 
out.put.4hr.wide[is.na(out.put.4hr.wide)]=0

out.put.wide <- out.put.1hr.wide %>% 
# save the table with all logistic model output fit values.
write.csv(out.put.1hr.wide, file.path(output.loc, "logistic_models_estimates.csv"), row.names = F)

# load network shapefile
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
names(roadnettb_snapped)

# append the model fit stats to the shapefile
roadnettb_snapped@data <- roadnettb_snapped@data %>% left_join(out.put.wide, by = c("RDSEG_ID")) # adding model output to the spatial data frame

# save the new shapefile
writeOGR(obj=roadnettb_snapped, dsn = output.loc, layer="Bellevue_roadnet_snapped_logistic_models_fit_stats", driver="ESRI Shapefile") # this is in geographical projection


# quick plot
plot(roadnettb_snapped, col = roadnettb_snapped@data$m08fit.08)
# legend("bottomright") # To do add legend, right now no idea of the color.



# Dan: I suggest we split out the visualization into separate scripts for simplicity, and have the analysis script end by saving a .RData of all the model outputs only.
# Jessie: good idea, this is just a temporary space for an quick examination of the visuals. if we think R could make some good visuals, we'll use a separate code to store these.


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

#EDA
Artclass_means_df <- w.all.yr.seg %>% group_by(ArterialCl) %>% summarise(me = mean(CrashPerMile))
ggplot(Artclass_means_df, aes(x=reorder(ArterialCl, -me), y=me)) + 
  geom_bar(stat="identity", fill = "#4682b4") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title="Average Collisions/Mile by Segment Type",y = 'Average Collisions/Mile', x = 'Segment Type') 

plot(w.all.yr.seg$uniqueCrashreports~as.numeric(w.all.yr.seg$medCirMagVar))
plot(w.all.yr.seg$uniqueCrashreports~as.numeric(w.all.yr.seg$nWazeJam))
plot(w.all.4hr.wd$uniqueCrashreports~as.numeric(w.all.4hr.wd$nWazeJam))
plot(w.all.yr.seg$nWazeJam~as.numeric(w.all.yr.seg$medCirMagVar))
plot(w.all.yr.seg$nWazeJam~as.numeric(w.all.yr.seg$meanCirMagVar))
