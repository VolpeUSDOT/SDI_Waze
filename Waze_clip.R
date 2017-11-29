# Waze clip
# Filtering Waze data for MD based on US Census Bureau polygon for the state
# Started 2017-11-29

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(sp)
library(maps) # for mapping base layers
library(reshape)
library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
library(raster)

wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/By month"
wazedir <- "W:/SDI Pilot Projects/Waze/Working Documents"
outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/By month_MD_only"

setwd(wazedir)

# Read in Waze data
# Files are >250 Mb. Try fread for faster import; not much faster in this case

# d <- read.csv(file.path(wazemonthdir, dir(wazemonthdir)[5])) # July 

# Read in county data from Census
counties <- readOGR("CensusCounty/.")
md_counties <- counties[counties$STATEFP == 24,]

monthfiles <- dir(wazemonthdir)

starttime <- Sys.Date()

# <><><><><><><><><><><><><><><><><><><><>
# Start loop ----
for(i in monthfiles){

  system.time(d <- read.csv(file.path(wazemonthdir, i))) # 6 min for July

# <><><><><><><><><><><><><><><><><><><><>
# Convert data into comparable classes ----
# Make the waze data into a SpatialPointsDataFrame for comparison with the state polygon.
  
  # Discard unneeded columns
  dropcols <- match(c("V1","X", "X.1", "X.2"), names(d))
  d <- d[,is.na(match(1:length(d), dropcols))]
  
  # Read datetime correctly
  d$time <- as.POSIXct(d$pubMillis/1000, origin = "1970-01-01", tz="America/New_York") # Time zone will need to be correctly configured for other States.
  
  # Rename lat long columns correctly and convert class
  names(d)[grep("location.x", names(d))] = "lon"
  names(d)[grep("location.y", names(d))] = "lat"
  
  d <- SpatialPointsDataFrame(d[c("lon","lat")], d)
  
  # Overlay points in polygons ----
  
  # Use over() from sp to join these to the census polygon. Points in Polygons, pip
  
  proj4string(d) <- proj4string(md_counties) # !!! Assumes Waze lat long are in NAD83 datum; check this.
  
  md_pip <- over(d, md_counties[,"NAME"]) # Match a county name to each row in d. If NA, it is not in Maryland.
  
  d$county <- as.character(md_pip$NAME)
  
  dx <- d@data[!is.na(d$county),] # Drop rows with no matching county name

  d <- SpatialPointsDataFrame(dx[c("lon","lat")], dx)

# <><><><><><><><><><><><><><><><><><><><>
# Export ----
# Save as .csv, .RData, and ShapeFile
  irda <- sub("csv", "RData", i)
  
  write.csv(d@data, file = file.path(outputdir, i), row.names = F)
  save(d, file = file.path(outputdir, i) )
  writeOGR(obj=d, dsn=outputdir, driver="ESRI Shapefile")
  
  cat(i, "complete \n", Sys.time()-starttime, "elapsed \n\n")
  
}
