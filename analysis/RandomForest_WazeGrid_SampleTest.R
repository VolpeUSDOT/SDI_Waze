# Running full set of random forest models - Here testing different sampling, 70/30 by day and by week
# https://github.com/VolpeUSDOT/SDI_Waze/wiki/Models-to-test

library(randomForest)
library(foreach) # for parallel implementation
library(doParallel) # includes iterators and parallel
library(aws.s3)
library(tidyverse)
library(rgdal)

# Run this if you don't have these packages:
# install.packages(c("rpart", "randomForest", "maptree", "party", "partykit", "rgdal", "foreach", "doParallel"), dep = T)

codeloc <- "~/SDI_Waze" 
# Set grid size:
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids
do.months = c("04","05","06","07","08","09")

REASSESS = F # re-assess model fit and diagnostics using reassess.rf instead of do.rf

inputdir <- "WazeEDT_RData_Input"
outputdir <- paste0("WazeEDT_Agg", HEXSIZE, "mile_RandForest_Output")

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

# 70/30 by day and week ----

# 23 Add all together
modelno = "23_original"

omits = c(alwaysomit,
#          "wx",
#          c("CRASH_SUM", "FATALS_SUM"), # FARS variables, 
#          grep("F_SYSTEM", names(w.04), value = T), # road class
#          c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
#          grep("WAC", names(w.04), value = T), # Jobs workplace
#          grep("RAC", names(w.04), value = T), # Jobs residential
          grep("MagVar", names(w.04), value = T), # direction of travel
          grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          grep("nWazeJam_", names(w.04), value = T) # neighboring jams
)

  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, 
                              omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                              model.no = modelno, rf.inputs = rf.inputs) 
modelno = "23_day"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "day",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
modelno = "23_week"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "week",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 

save("keyoutputs", file = paste0("Output_to_", modelno))

  
# 30 Add all (but not Waze event subtypes or sub-subtypes)
modelno = "30_original"
omits = c(alwaysomit, alert_subtypes
          #"wx",
          #c("CRASH_SUM", "FATALS_SUM"), # FARS variables, added in 19
          #grep("F_SYSTEM", names(w.04), value = T), # road class
          #c("MEAN_AADT", "SUM_AADT", " SUM_miles"), # AADT
          #grep("WAC", names(w.04), value = T), # Jobs workplace
          #grep("RAC", names(w.04), value = T), # Jobs residential
          #grep("MagVar", names(w.04), value = T), # direction of travel
          #grep("medLast", names(w.04), value = T), # report rating, reliability, confidence
          #grep("nWazeAcc_", names(w.04), value = T), # neighboring accidents
          #grep("nWazeJam_", names(w.04), value = T) # neighboring jams
          )

  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, omits, response.var = "MatchEDT_buffer_Acc",  
                                model.no = modelno, rf.inputs = rf.inputs) 
  save("keyoutputs", file = paste0("Output_to_", modelno))

modelno = "30_day"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "day",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
modelno = "30_week"
  
  keyoutputs[[modelno]] = do.rf(train.dat = w.04_09, split.by = "week",
                                omits = c(omits), response.var = "MatchEDT_buffer_Acc", 
                                model.no = modelno, rf.inputs = rf.inputs) 
  
save("keyoutputs", file = paste0("Output_to_", modelno))
  
  
### Stratification and downsampling ----

# Again, use models 23 and 30. Implement two kinds of stratified sampling to investigate unbalanced data adjustments.
# From randomForest documentation with notes:
# strata: A (factor) variable that is used for stratified sampling.
# --> As a classication problem with one main factor of interest, our strata will be the response variable, MatchEDT_buffer_Acc. Other strata to consider could be hour of day, since we know that drives the number of Waze observations.
# sampsize: Size(s) of sample to draw. For classification, if sampsize is a vector of the length the number of strata, then sampling is stratified by strata, and the elements of sampsize indicate the numbers to be drawn from the strata.
# 

# Explore imbalance ----
# Actual sample size imbalance:
table(w.04_09$MatchEDT_buffer)
table(w.04_09$MatchEDT_buffer) / nrow(w.04_09)

EDT.hr <- table(w.04_09$MatchEDT_buffer, w.04_09$hour)
EDT.hr.pct <- apply(EDT.hr, 2, function(x) 100*x/sum(x))
matplot(t(EDT.hr.pct),
        type = "l", main = "Imbalance by hour",
        ylab = "Pct of data in class",
        xlab = "Hour of day",
        ylim = c(0, 100))

# long format using dplyr
hr.1 <- w.04_09 %>%
  group_by(hour, MatchEDT_buffer) %>%
  summarise(n = n())  %>%
  mutate(freq = 100 * n / sum(n))

hr.1$text <- paste("Hour:", hr.1$hour, "\n EDT crash:", hr.1$MatchEDT_buffer,
                   "\n Frequency:", round(hr.1$freq, 2))

gp <- ggplot(hr.1, aes(x = hour, 
                       y = freq, 
                       group = MatchEDT_buffer, 
                       text = text)) +
 # geom_point() +
  ylab("Frequency") + xlab("Hour of Day") + theme_bw() + 
  ggtitle("Class imbalance by time") +
  geom_step(aes(color = MatchEDT_buffer), lwd = 2) 
  
gp 

maxval <- hr.1 %>% 
  group_by(MatchEDT_buffer) %>% 
  summarise(max.freq = max(freq), 
            hr.max = hour[which(freq == max.freq)],
            min.freq = min(freq),
            hr.min= hour[which(freq == min.freq)])

plot.freq = c(maxval$max.freq, maxval$min.freq) + c(-3, +3, -3, +3)

gp2 <- gp + annotate("text", 
              x = c(maxval$hr.max, maxval$hr.min), 
              y = plot.freq, 
              label = round(c(maxval$max.freq, maxval$min.freq), 2)) + 
  guides(color=guide_legend(title="EDT crash class"))
gp2
# library(plotly); ggplotly(gp2, tooltip = "text") %>% layout(hovermode = 'x')
# Imbalance maximal at 2-4am (97/3), minimal at 4-5 pm (still 94/6), as expected

# What should sampling size be? Try a few different versions. Most extreme, match exactly the rare class size. Currently maximum imbalance by frequency is 42:1. So try 10:1, 5:1, and 1:1 sampling. Rather than modify do.rf function, running it 'manually' here for now.
# Use cutoff of .5 to start, and look at outputs.

# Run stratification Model 30 ----
omits = c(alwaysomit, alert_subtypes)

# Setup for the model; pulled out of do.rf() function
response.var = "MatchEDT_buffer"
train.dat = w.04_09
class(train.dat) <- "data.frame"
test.split = .30
fitvars <- names(train.dat)[is.na(match(names(train.dat), omits))]
mtry.use = rf.inputs$mtry
trainrows <- sort(sample(1:nrow(train.dat), size = nrow(train.dat)*(1-test.split), replace = F))
testrows <- (1:nrow(train.dat))[!1:nrow(train.dat) %in% trainrows]
rundat = train.dat[trainrows,]
test.dat.use = train.dat[testrows,]

# Find the number of "rare" classes, EDT crashes, in this sample
nRareClass = min(table(rundat[,response.var]))

modelno = "30_10"


# Start RF in parallel
starttime = Sys.time()

cl <- makeCluster(rf.inputs$avail.cores, useXDR = F) 
registerDoParallel(cl)

rf.30_10 <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores), .combine = randomForest::combine, .multicombine=T, .packages = 'randomForest') %dopar% 
  randomForest(x = rundat[,fitvars], y = rundat[,response.var], 
               ntree = ntree, mtry = mtry.use, 
               maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
               keep.forest = T,
               strata = rundat[,response.var],
               sampsize = c(10*nRareClass, nRareClass)
               )

timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "to fit model", modelno, "\n")

modelno = "30_05"

rf.30_05 <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores), .combine = randomForest::combine, .multicombine=T, .packages = 'randomForest') %dopar% 
  randomForest(x = rundat[,fitvars], y = rundat[,response.var], 
               ntree = ntree, mtry = mtry.use, 
               maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
               keep.forest = T,
               strata = rundat[,response.var],
               sampsize = c(5*nRareClass, nRareClass)
  )

timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "to fit model", modelno, "\n")

modelno = "30_01"

rf.30_01 <- foreach(ntree = rep(rf.inputs$ntree.use/rf.inputs$avail.cores, rf.inputs$avail.cores), .combine = randomForest::combine, .multicombine=T, .packages = 'randomForest') %dopar% 
  randomForest(x = rundat[,fitvars], y = rundat[,response.var], 
               ntree = ntree, mtry = mtry.use, 
               maxnodes = rf.inputs$maxnodes, nodesize = rf.inputs$nodesize,
               keep.forest = T,
               strata = rundat[,response.var],
               sampsize = c(1*nRareClass, nRareClass)
  )

stopCluster(cl); rm(cl); gc(verbose = F) # Stop the cluster immediately after finished the RF

timediff = Sys.time() - starttime
cat(round(timediff,2), attr(timediff, "unit"), "to fit model", modelno, "\n")
# End RF in parallel

savelist = c("rf.30_10", "rf.30_05", "rf.30_01", "trainrows", "testrows") 

s3save(list = savelist,
       object = file.path(outputdir, "Stratification_test_RandomForest_Output.RData"),
       bucket = waze.bucket)
