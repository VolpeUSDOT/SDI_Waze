# Waze clip
# Filtering Waze data for MD based on US Census Bureau polygon for the state
# Started 2017-11-29
# Now working in SDC

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

state = "MD"

# read functions
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Check for package installations
source(file.path(codeloc, 'utility/get_packages.R'))


# Location of SDC SDI Waze team S3 bucket. Monthly files live here
teambucket <- "prod-sdc-sdi-911061262852-us-east-1-bucket"

inputdir <- paste0("Raw_", state)
outputdir <- paste0("Clip_", state) 
localdir <- "/home/daniel/workingdata" # location on this instance to hold working data files that can be s3loaded. *Need full path, cannot abbreviate to ~/workingdata, for readOGR*
system(paste('mkdir', localdir))

library(sp)
library(maps) # for mapping base layers
library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
library(raster)

BUFFER = TRUE # Set to false to use original md_counties shapefile, true for 0.5 mi buffered version

setwd(localdir)

# Read in county data from Census
if(BUFFER){
  md_counties <- readOGR("Census Files", layer = "MD_buffered")  
  
} else {
  counties <- readOGR("Census Files", layer = "cb_2015_us_county_500k")
  md_counties <- counties[counties$STATEFP == 24,]
}

monthfiles <- dir(wazemonthdir)[grep("RData", dir(wazemonthdir))]

# <><><><><><><><><><><><><><><><><><><><>
# Start loop
for(i in monthfiles){ # i = "MD__2017-04.RData"
  
  starttime <- Sys.time()
  
  system.time(load(file.path(wazemonthdir, i))) # 3-4 min for July, up to 4x slower on VPN. 
  # 25s to load 50 MB RData file on Volpe network; 5-8x slower on VPN
  
  cat(i, " loaded \n")
  
  d <- mb # was abbreviated 'monthbind', use 'd' as before
  
  # original file size in MB
  orig.file.size <- round(file.info(file.path(wazemonthdir, i))$size/1000000, 2)
  orig.nrow <- nrow(d)
  # <><><><><><><><><><><><><><><><><><><><>
  # Convert data into comparable classes
  # Make the waze data into a SpatialPointsDataFrame for comparison with the state polygon.
  
  # Discard unneeded columns. Discarding two date colums bc have pubMillis, will make a POSIX datetime column from this. For now, also dropping 'filename', revisit this.
  dropcols <- match(c("V1","X", "X.1", "X.2", "X.3", "pubMillis_date", "date", "filename"), names(d))
  d <- d[,is.na(match(1:ncol(d), dropcols))]
  
  d <- SpatialPointsDataFrame(d[c("lon","lat")], d)
  
  # Overlay points in polygons
  
  # Use over() from sp to join these to the census polygon. Points in Polygons, pip
  
  proj4string(d) <- proj4string(md_counties) # !!! Assumes Waze lat long are in NAD83 datum; check this.
  
  matchcol = ifelse(BUFFER, "MD", "NAME")
  
  md_pip <- over(d, md_counties[,matchcol]) # Match a county name to each row in d. If NA, it is not in Maryland.
  
  d@data <- data.frame(d@data, matchcol = as.character(md_pip[,matchcol]))
  names(d@data)[length(d@data)] = matchcol
  
  dx <- d@data[!is.na(d@data[,matchcol]),] # Drop rows with no matching county name
  
  d <- SpatialPointsDataFrame(dx[c("lon","lat")], dx)
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Export
  # Save as .csv, .RData, and ShapeFile
  
  if(BUFFER) i <- sub("MD", "MD_buffered", i)
  irda <- i
  ishp <- sub("\\.RData", "", i)
  icsv <- sub("RData", "csv", i)
  
  save(list = "d", file = file.path(outputdir_temp, irda) )
  
  write.csv(d@data, file = file.path(outputdir_temp, icsv), row.names = F)
  
  #  writeOGR(obj=d, dsn=file.path(outputdir, ishp), driver="ESRI Shapefile")
  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
  
  new.file.size <- round(file.info(file.path(outputdir_temp, i))$size/1000000, 2)
  new.nrow <- nrow(d@data)
  
  cat("File reduced from", orig.file.size, "MB to", new.file.size, "MB \n\n",
      format(orig.nrow - new.nrow, big.mark =",)", 
             "rows of out-of-state data removed \n\n", rep("<>",20), "\n")
      
}

filelist <- dir(outputdir_temp)[grep("[RData$|csv$]", dir(outputdir_temp))]
