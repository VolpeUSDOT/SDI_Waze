# Waze clip
# Filtering Waze data for each state based on 0.5 mile buffered US Census Bureau polygon for the state
# Started 2017-11-29
# Now working in SDC, as a call from ReduceWaze_SDC.R. This 

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

# Values passed from ReduceWaze_SDC.R:
# states = vector of state abbreviations
# teambucket = S3 bucket name
# codeloc; functions from utilities/wazefunctions.R
# output.loc = ~/tempout, place to store temporary files
# localdir = /home/daniel/workingdata, full path to place for working data, including spatial layers
# yearmonths, yearmonths.1, yearmonths.end

# system(paste0("aws s3 ls ", teambucket, "/MD/"))

library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
library(raster)

user <- paste0( "/home/", system("whoami", intern = TRUE)) 
localdir <- paste0(user, "/workingdata/") # full path for readOGR
setwd(localdir)

# Spatial setup ----

BUFFER = TRUE # Set to false to use original counties shapefile, true for 0.5 mi buffered version

# Project to Albers equal area, ESRI 102008
proj <- showP4(showWKT("+init=epsg:102008"))

# inherits states from ReduceWaze_SDC.R

FIPS = maps::state.fips %>% filter(abb %in% states)
FIPS = FIPS[!duplicated(FIPS$fips),c("abb", "fips")]
names(FIPS) = c("state", "FIPS")
FIPS$FIPS <- formatC(FIPS$FIPS, width = 2, flag = "0")

# Start loop over states ----

statefiles <- dir(output.loc)[grep("RData", dir(output.loc))]

starttime <- Sys.time()

for(i in states){ # i = "CT"
  
  # Read in county data from Census. Check to see if a buffered version for this state exists, create if not.
  if(BUFFER){
    if(length(grep(paste0(i, "_buffered"), dir("census")))==0) { source(codeloc, "datacleaning/Buffer_state.R") }  
    i_counties <- readOGR("census", layer = paste0(i, "_buffered"))  
  } else {
    counties <- readOGR("census", layer = "cb_2017_us_county_500k")
    counties <- spTransform(counties, CRS(proj))
    i_counties <- counties[counties$STATEFP == FIPS$FIPS[FIPS$state == i],]
  }
  
  # ~ 1 min to read 3.4 Gb CT file
  # Sort for latest version
  statefile = sort(statefiles[grep(paste0(i, "_Raw_"), statefiles)], decreasing = T)[1]
  load(file.path(output.loc, statefile))
  
  cat(i, "loaded \n")
  
  # original file size in MB
  orig.file.size <- round(file.info(file.path(output.loc, statefile))$size/1000000, 2)
  orig.nrow <- nrow(results)
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Convert data into comparable classes
  # Make the rowname pointer for Waze data into a SpatialPoints object for comparison with the state polygon.
  lon.lat <- data.frame(as.numeric(results$location_lon),
                        as.numeric(results$location_lat))

  dx <- SpatialPoints(coords = lon.lat, proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  if(!identical(proj4string(dx), proj4string(i_counties))){
    dx <- spTransform(dx, CRS(proj4string(i_counties)))
  }
  
  # Overlay points in polygons
  # Use over() from sp to join these to the census polygon. Points in Polygons, pip
  matchcol = ifelse(BUFFER, "State", "NAME")
  
  # State buffer name to each row in d. If NA, it is not in the polygon.
  # could be parallelized by chunking the pointer dx and then combining together
  i_pip <- over(dx, i_counties[,matchcol]) 
  
  # subset results for only points in state
  r2 <- results[!is.na(i_pip[,matchcol]),]
  rm(results, i_pip, lon.lat); gc() # remove to save memory
  
  d <- SpatialPointsDataFrame(coords = data.frame(as.numeric(r2$location_lon),
                                                  as.numeric(r2$location_lat)),
                              r2,
                              proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  if(!identical(proj4string(d), proj4string(i_counties))){
    d <- spTransform(d, CRS(proj4string(i_counties)))
  }
  
  # Check with plot
  plot(i_counties, main = paste("Plotting original and clipped", i))
  plot(dx[sample(1:length(dx), size = 10000),], add = T, col = alpha("firebrick", 0.3))
  plot(d[sample(1:nrow(d), size = 10000),], add = T, col = alpha("midnightblue", 0.1))
  dev.print(jpeg, file = paste0("Figures/Clip_plot_", i, ".jpeg"), width= 500, height = 500)
  
  rm(i_counties, dx, r2)
  # <><><><><><><><><><><><><><><><><><><><>
  # Export as .RData
  # Possibly save as .csv and ShapeFile; takes time and disk space, skip for now.
  
  fn = paste0(i, "_Clipped_events_to_", yearmonths[length(yearmonths)])
  
  if(BUFFER) fn <- sub("_Clipped", "_Buffered_Clipped", fn)

  save(list = "d", file = file.path(localdir, paste0(fn, ".RData")))
  
  # write.csv(d@data, file = file.path(localdir, paste0(fn, ".csv")), row.names = F)
   
  # writeOGR(obj = d, dsn = localdir, layer = fn, driver="ESRI Shapefile")
   
  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
  
  new.file.size <- round(file.info(file.path(localdir, paste0(fn, ".RData")))$size/1000000, 2)
  new.nrow <- nrow(d@data)
  
  cat(i, "file changed from", orig.file.size, "MB to", new.file.size, "MB \n\n",
      format(orig.nrow - new.nrow, big.mark =","), 
             "rows of out-of-state data removed \n\n", rep("<>",20), "\n")
  
  # Copy to S3
  system(paste("aws s3 cp",
               file.path(localdir, paste0(fn, ".RData")),
               file.path(teambucket, i, paste0(fn, ".RData"))))
  
  rm(d); gc()
  
} # end state loop

rm(counties, i.u, xx)
