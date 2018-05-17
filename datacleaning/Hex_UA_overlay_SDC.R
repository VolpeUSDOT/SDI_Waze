# Assigns urban area classifications and hexagonal grid IDs to Waze and EDT events
# Merges the two files into a matched data frame and saves
# Loops over all months of available data where both EDT and Waze files exist, for each state

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))


library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(foreach)
library(doParallel)

output.loc <- "~/tempout"
localdir <- "/home/daniel/workingdata/" # full path for readOGR
edtdir <- normalizePath(file.path(localdir, "EDT"))
wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"),
                  stringsAsFactors = F)


# Project to Albers equal area conic 102008. Check comparision with USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version, used for producing hexagons: 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

setwd(localdir)

TEST = F      # Change to F to run for all available months, T for only a subset of months
CHECKPLOT = T # Make plots for each stage, to make sure of spatial overlay

# Read in spatial data ----

# Read in urban/rural layer from Census
# From: https://www.census.gov/geo/maps-data/data/cbf/cbf_ua.html
# https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshprd13/TGRSHPrd13_TechDoc_A.pdf
# Get from S3 if necessary

ua <- readOGR(file.path(localdir, "census"), layer = "cb_2016_us_ua10_500k")
ua <- spTransform(ua, CRS(proj.USGS))

# Read in county shapefile, and apply coordinate reference system of hexagons (USGS version of Albers equal area) to Urban Areas and counties using uproj.USGS
co <- readOGR(file.path(localdir, "census"), layer = "cb_2017_us_county_500k")
co <- spTransform(co, CRS(proj.USGS))


# Read in hexagon shapefiles. This is a rectangular surface of 1 sq mi area hexagons, national
# load("~/workingdata/Hex/hexagons_1mi_lower48_neighbors.RData")
# hex <- readOGR(file.path(localdir, "Hex"), layer = "hexagons_1mi_lower48_neighbors")
# In terminal, ulimit -s 16384, then R readOGR, save as .RData. Produces 4.5 Gb .Rdata file.
# !!! Now doing just for these four states with separate files !!!

# start state loop ----
for(i in states){ # i = "UT"
  
  if(i == "MD"){
    hex = readOGR(file.path(localdir, "Hex", "MD_hexagons_shapefiles"), layer = "MD_hexagons_1mi_newExtent_neighbors")
  } else {
    hex = readOGR(file.path(localdir, "Hex"), layer = paste0(i, "_hexagons_1mi_neighbors"))
  }
  
  # grid column names
  gridcols <- names(hex)[grep("^GRID", names(hex))]
  
  FIPS_i = formatC(state.fips[state.fips$abb == i, "fips"][1], width = 2, flag = "0")
  co_i <- co[co$STATEFP == FIPS_i,]
  
  # Files -- need to add step to grab from S3 if not in these locations. Also change from tempout to a subdirectory in workingdata
  e_i <- read.csv(file.path(edtdir, dir(edtdir)[grep(i, dir(edtdir))]),
                    na.strings = c("NA", "NULL"))
  
  # Prep EDT data for this state
  e_i <- e_i[!is.na(e_i$GPSLat) & !is.na(e_i$GPSLong),]
  e_i <- e_i[!e_i$GPSLat > 77 & !e_i$GPSLong < -777,]
  e_i <- e_i[e_i$CrashState == state.name[state.abb == i],]
  
  use.tz <- tzs$tz[tzs$states == i]
  
  e_i$CrashDate_Local <- with(e_i,
                              strptime(
                                paste(substr(CrashDate, 1, 10),
                                      HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = use.tz)
  )
  
  
  edtmonths <- sort(unique(format(e_i$CrashDate_Local, "%Y-%m")))
  
  # project EDT
  if(class(e_i)=="data.frame"){
    e_i <- SpatialPointsDataFrame(e_i[c("GPSLong", "GPSLat")], e_i, 
                                  proj4string = CRS("+proj=longlat +datum=WGS84"))  
    
    e_i <-spTransform(e_i, CRS(proj.USGS))
  }
  
  # Names of files for state i, in a directory with all clipped, buffered, unique uuid events by month.
  wazemonthfiles <- dir(wazedir)[grep(i, dir(wazedir))]
  wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
  wazemonths <- sort(unique(substr(wazemonthfiles, 4, 10)))
  
  months_shared <- edtmonths[edtmonths %in% wazemonths]
  # make sure months are actually shared 
  stopifnot(all(months_shared == wazemonths[wazemonths %in% edtmonths]))
  
  avail.cores <- parallel::detectCores()
  if(avail.cores > length(months_shared)) avail.cores = length(months_shared) # use only cores necessary
  cl <- makeCluster(avail.cores) 
  registerDoParallel(cl)
  
  starttime_state <- Sys.time()
  
  # Start parallel loop over yearmonths within state ----
  foreach(mo = months_shared, .packages = c("dplyr", "tidyr","sp","rgdal","scales")) %dopar% {
    # mo = "2018-04" 
    
    # Waze: Comes from aggregated monthly Waze events, clipped to a 0.5 mile buffer around state i, for each month.  All the waze files share the same object name, mb
    load(file.path(wazedir, wazemonthfiles[grep(mo, wazemonthfiles)]))
    if(class(mb)=="data.frame"){
      mb <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                                    proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure Waze data is a SPDF
      
      mb <-spTransform(mb, CRS(proj.USGS))
    }
    
    link <- read.csv(file.path(localdir, "Link", paste0("EDT_Waze_link_", mo,"_", i, ".csv")))
    
    # Add "M" code to the table to show matches if we merge in full datasets
    match <- rep("M", nrow(link))
    link <- mutate(link, match) 
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Overlay points in polygons
    # Use over() from sp to join these to the census polygon. Points in Polygons, pip
    
    # First clip EDT to state 
    EDT.co <- over(e_i, co_i["COUNTYFP"])
    e_i<- e_i[!is.na(EDT.co$COUNTYFP),] 
    # <><><><><><><><><><><><><><><><><><><><>
    # Urban area overlay ----
    
    waze_ua_pip <- over(mb, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in mb. 
    edt_ua_pip <- over(e_i, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in e_i. 
    
    mb@data <- data.frame(mb@data, waze_ua_pip)
    names(mb@data)[(length(mb@data)-1):length(mb@data)] <- c("Waze_UA_Name", "Waze_UA_Type")
    
    e_i@data <- data.frame(e_i@data, edt_ua_pip)
    names(e_i@data)[(length(e_i@data)-1):length(e_i@data)] <- c("EDT_UA_Name", "EDT_UA_Type")
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Hexagon overlay ----
    
    waze_hex_pip <- over(mb, hex[,gridcols]) # Match hexagon names to each row in mb. 
    edt_hex_pip <- over(e_i, hex[,gridcols]) # Match hexagon names to each row in e_i. 
    # add these columns to the data frame
    mb@data <- data.frame(mb@data, waze_hex_pip)
    
    e_i@data <- data.frame(e_i@data, edt_hex_pip)
    
    # Check:
    if(CHECKPLOT){ 
      jpeg(file = file.path(localdir, paste0("Figures/Checking_EDT_Waze_UAoverlay_",mo, "_", i, ".jpg")), width = 500, height = 500) 
      plot(mb, main = paste("Pre-linking EDT and Waze", mo, i))
      points(e_i, col = alpha("red", 0.2))
      plot(co_i, add = T, col = alpha("grey80", 0.1))
      dev.off()
    }
    # <><><><><><><><><><><><><><><><><><><><>
    # Merge and save EDT-Waze
    # <><><><><><><><><><><><><><><><><><><><>
    
    # Save Waze data as dataframe
    waze.df <- mb@data 
    # names(waze.df)
    
    # Save EDT data as dataframe
    edt.df <- e_i@data 
    # names(edt.df)
    
    # before carrying out the join, rename the EDT grid cell columns 
    names(edt.df)[grep("^GRID", names(edt.df))] <- paste(names(edt.df)[grep("^GRID", names(edt.df))], "edt", sep = ".")
    
    # Join Waze data to link table (full join)
    link.waze <- full_join(link, waze.df, by=c("id.incidents"="alert_uuid"))
    
    # Add W code to match column to indicate only Waze data
    link.waze$match <- ifelse(is.na(link.waze$match), 'W', link.waze$match)
    
    # Join EDT data to Waze-link table (full join)
    # *** Potential improvement for data storage: at this step, make a selection of only those columns which will be useful from EDT (do not include all 0 columns or columns not useful for the model) ***
    
    edt.df$CrashDate_Local <- as.character(edt.df$CrashDate_Local) # will need to convert back to POSIX
    link.waze.edt <- full_join(link.waze, edt.df, by = c("id.accident"="ID")) 
    
    #Add E code to match column to indicate only EDT data
    link.waze.edt$match <- ifelse(is.na(link.waze.edt$match), 'E', link.waze.edt$match)
    # table(link.waze.edt$match)
    
    # Convert CrashDate_Local back to POSIX
    link.waze.edt$CrashDate_Local <- strptime(link.waze.edt$CrashDate_Local, "%Y-%m-%mb %H:%M:%S", tz = use.tz)
    
    # rename ID variables for compatibility with existing code
    names(link.waze.edt)[grep("id.incident", names(link.waze.edt))] = "uuid.waze"
    names(link.waze.edt)[grep("id.accident", names(link.waze.edt))] = "ID"
    
    # save the merged file  to temporary output directory, move to S3 after
    # write.csv(link.waze.edt, file=file.path(localdir, "Overlay", paste0("merged.waze.edt.", mo, "_", i, ".csv")), row.names = F)
    fn = paste0("merged.waze.edt.", mo, "_", i, ".RData")
    
    save(list=c("link.waze.edt", "edt.df"), file = file.path(localdir, "Overlay", fn))
    
    system(paste("aws s3 cp",
                 file.path(localdir, "Overlay", fn),
                 file.path(teambucket, i, fn)))
    
    if(CHECKPLOT) { 
      lwe.sp <- SpatialPoints(link.waze.edt[!is.na(link.waze.edt$lon) | !is.na(link.waze.edt$lat),c("lon", "lat")],
                                   proj4string = CRS("+proj=longlat +datum=WGS84")) 
      lwe.sp <-spTransform(lwe.sp, CRS(proj.USGS))
      jpeg(file = file.path(localdir, paste0("Figures/Checking2_EDT_Waze_UAoverlay_",mo, "_", i, ".jpg")), width = 500, height = 500) 
      plot(co_i, main = paste("Linked EDT-Waze", mo, i))
      plot(lwe.sp, col = alpha("blue", 0.3), add=T, pch = "+")
      dev.off()
    }  
  } # End month loop
  
  timediff <- Sys.time() - starttime_state
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to overlay", i, "\n\n")
  
} #End state loop

stopCluster(cl)
