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

VERSION = 2 # Choose which type of aggregation and clipping to do. 1 was for original aggregated files from Lia; 2 is from .RData files; 3 is for both EDT and Waze, clipping to a 5 x 5 mi square


# moving files from a temporary directory on local machine to shared drive. 
# Files are removed from the local machine by this process.
movefiles <- function(filelist, temp = outdir, wazedir){
  for(i in filelist){
    # Fix path separators for Windows / R 
    temp <- gsub("\\\\", "/", temp)
    temp <- gsub("C:/U", "C://U", temp)
    
    # Encase the destination path in quotes, because of spaces in path name
    system(paste0("mv ", file.path(temp, i), ' \"', file.path(wazedir, i), '\"'))
    
  }
}



# Version 1: from .csv aggregations made by Lia ----

if(VERSION == 1){
  
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
  # Start loop
  for(i in monthfiles[c(2, 3, 4, 6, 7, 8)]){ # change back to all files when done with manual steps
    
    starttime <- Sys.time()
    
    system.time(d <- read.csv(file.path(wazemonthdir, i))) # 3-4 min for July, up to 4x slower on VPN
    
    # original file size in MB
    orig.file.size <- file.info(file.path(wazemonthdir, i))$size/1000000
    orig.nrow <- nrow(d)
    # <><><><><><><><><><><><><><><><><><><><>
    # Convert data into comparable classes
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
    
    # Overlay points in polygons
    # Use over() from sp to join these to the census polygon. Points in Polygons, pip
    
    proj4string(d) <- proj4string(md_counties) # !!! Assumes Waze lat long are in NAD83 datum; check this.
    
    md_pip <- over(d, md_counties[,"NAME"]) # Match a county name to each row in d. If NA, it is not in Maryland.
    
    d$county <- as.character(md_pip$NAME)
    
    dx <- d@data[!is.na(d$county),] # Drop rows with no matching county name
    
    d <- SpatialPointsDataFrame(dx[c("lon","lat")], dx)
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Export
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
  
} # end version if statement

## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
# Version 2: From .RData aggregated Month file ----
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

if(VERSION == 2){
  
  BUFFER = TRUE # Set to false to use original md_counties shapefile, true for 0.5 mi buffered version
  
  wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD"
  wazedir <- "W:/SDI Pilot Projects/Waze/Working Documents"
  # create a temporary directory on the C: drive of the local machine to save files
  outputdir_temp <- tempdir()
  
  outputdir_final <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
  
  setwd(wazedir)
  
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
  
  movefiles(filelist, outputdir_temp, outputdir_final)
  
} # end version if statement


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
# Version 3: From .RData aggregated Month file, clipping to 5 mi box ----
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

if(VERSION == 3){
  
  wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD"
  outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
  
  setwd("//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI")
  
  # Read in 5 mile clipping area polygon. This is a kmz file manually created in Google Earth
  
  fivemi <- readOGR("spatial_layers/5miClip.kml", "5miClip")
  
  monthfiles <- dir(wazemonthdir)[grep("RData", dir(wazemonthdir))]
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Start 
  #for(i in monthfiles){ # 
  i = "MD__2017-04.RData"
  
  starttime <- Sys.time()
  
  system.time(load(file.path(wazemonthdir, i))) # 3-4 min for July, up to 4x slower on VPN. 
  # 25s to load 50 MB RData file on Volpe network; 8x slower on VPN
  
  d <- mb # was abbreviated 'monthbind', use 'd' as before
  
  # original file size in MB
  orig.file.size <- file.info(file.path(wazemonthdir, i))$size/1000000
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
  
  proj4string(fivemi) <- proj4string(d) 
  
  md_pip <- over(d, fivemi) # Match to our bounding box
  
  d$fivemi <- as.character(md_pip$Name)
  
  dx <- d@data[!is.na(d$fivemi),] # Drop rows not inside the box
  
  d <- SpatialPointsDataFrame(dx[c("lon","lat")], dx)
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Export
  # Save as .csv and .RData
  irda <- paste("FiveMiSubset", i, sep = "_")
  icsv <- sub("RData", "csv", irda)
  
  save(list = "d", file = file.path(outputdir, irda) )
  
  write.csv(d@data, file = file.path(outputdir, icsv), row.names = F )
  # Clip EDT to April, same box
  
  edtdir = "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/edt_data/2016_01_to_2017_09_MD_and_IN"
  
  i = "1_CrashFact_edited.txt"
  starttime <- Sys.time()
  
  system.time(e <- read.csv(file.path(edtdir, i), sep = "\t")) 
  
  d <- e # was abbreviated 'monthbind', use 'd' as before
  
  # original file size in MB
  orig.file.size <- file.info(file.path(edtdir, i))$size/1000000
  orig.nrow <- nrow(d)
  # <><><><><><><><><><><><><><><><><><><><>
  # Convert data into comparable classes
  # Make the EDT data into a SpatialPointsDataFrame for comparison with the polygon.
  
  # Discard unneeded columns. 
  dropcols <- match(c("GPSLong_OG"), names(d))
  d <- d[,is.na(match(1:ncol(d), dropcols))]
  
  # Discard rows with no lat long
  d <- d[!is.na(d$GPSLat),]
  
  # Keep only Maryland, only 2017
  d <- d[d$CrashState == "Maryland" & d$StudyYear == "2017",]
  
  # Format datetime. No crashdate_local, make it here
  d$CrashDate_Local <- with(d,
                            strptime(
                              paste(substr(CrashDate, 1, 10),
                                    HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = "America/New_York")
  )
  
  # Also all of April, unbounded by the box
  aprilonly <- format(d$CrashDate_Local, "%m") == "04"
  
  
  d <- SpatialPointsDataFrame(d[c("GPSLong_New","GPSLat")], d)
  
  edt.april <- d[aprilonly,] # write out edt.april below
  
  # Overlay points in polygons
  proj4string(fivemi) <- proj4string(d) 
  
  md_pip <- over(d, fivemi) # Match to our bounding box
  
  d$fivemi <- as.character(md_pip$Name)
  
  dx <- d@data[!is.na(d$fivemi),] # Drop rows not inside the box
  
  d <- SpatialPointsDataFrame(dx[c("GPSLong_New","GPSLat")], dx)
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Export
  # Save as .csv and .RData
  i <- sub("txt", "RData", i)
  irda <- paste("FiveMiSubset", "2017-04", i, sep = "_")
  icsv <- sub("RData", "csv", irda)
  
  edt.5mi <- d
  
  save(list = "edt.5mi", file = file.path(outputdir, irda) )
  
  write.csv(edt.5mi@data, file = file.path(outputdir, icsv), row.names = F)
  
  april.irda <- paste("2017-04", i, sep = "_")
  april.icsv <- sub("RData", "csv", april.irda)
  
  # edt.april 
  save(list = "edt.april", file = file.path(outputdir, april.irda) )
  
  write.csv(edt.april, file = file.path(outputdir, april.icsv), row.names = F)
  
} # end version if statement
