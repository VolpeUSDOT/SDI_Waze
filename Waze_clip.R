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
# Files are >250 Mb. Sometimes data.table::fread is faster for files like these; not in this case. Just use read.csv.

# Read in county data from Census
counties <- readOGR("CensusCounty/.")
md_counties <- counties[counties$STATEFP == 24,]

monthfiles <- dir(wazemonthdir)


monthfiles[c(2, 3, 4, 6, 7, 8)] # started w July, doing manually because connection slow


# <><><><><><><><><><><><><><><><><><><><>
# Start loop ----
for(i in monthfiles[c(2, 3, 4, 6, 7, 8)]){ # change back to all files when done with manual steps
  
  starttime <- Sys.time()
  
  system.time(d <- read.csv(file.path(wazemonthdir, i))) # 3-4 min for July, up to 4x slower on VPN
  
  # original file size in MB
  orig.file.size <- file.info(file.path(wazemonthdir, i))$size/1000000
  orig.nrow <- nrow(d)
# <><><><><><><><><><><><><><><><><><><><>
# Convert data into comparable classes ----
# Make the waze data into a SpatialPointsDataFrame for comparison with the state polygon.
  
  # Discard unneeded columns. Discarding two date colums bc have pubMillis, will make a POSIX datetime column from this. For now, also dropping 'filename', revisit this.
  dropcols <- match(c("V1","X", "X.1", "X.2", "X.3", "pubMillis_date", "date", "filename"), names(d))
  d <- d[,is.na(match(1:ncol(d), dropcols))]

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
  ishp <- sub("csv", "", i)

  save(list = "d", file = file.path(outputdir, irda) )
  
  write.csv(d@data, file = file.path(outputdir, i), row.names = F)
  
#  writeOGR(obj=d, dsn=file.path(outputdir, ishp), driver="ESRI Shapefile")
  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
  
  new.file.size <- file.info(file.path(outputdir, i))$size/1000000
  new.nrow <- nrow(d@data)
  
  cat(".csv reduced from", orig.file.size, "to", new.file.size, "\n\n",
      orig.nrow - new.nrow, "rows of out-of-state data removed \n\n", rep("<>",20), "\n")
  
}
