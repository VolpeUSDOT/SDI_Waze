# Assigns urban area classifications and hexagonal grid IDs to Waze and EDT events
# Merges the two files into a matched data frame and saves
# Loops over all months of available data where both EDT and Waze files exist, for each state

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
# Check for package installations
rm(list = ls())
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))


library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersects
library(foreach)
library(doParallel)

state = "TN"

output.loc <- "~/tempout"
home.loc <- getwd()
user <- if(length(grep("@securedatacommons.com", home.loc)) > 0) {
  paste0( "/home/", system("whoami", intern = TRUE), "@securedatacommons.com")
} else {
  paste0( "/home/", system("whoami", intern = TRUE))
} # find the user directory to use

localdir <- file.path(user, "workingdata") # full path for readOGR
crashdir <- normalizePath(file.path(localdir, state, "Crash"))
wazedir <- normalizePath(file.path(localdir, state, "Waze")) # has State_Year-mo.RData files. Grab from S3 if necessary

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

# Time zones
# TN has 2 timezones (eastern and central) - Waze data have time zones applied by a shapefile, in the third step of ReduceWaze_SDC.R now.

# Project to Albers equal area conic 102008. Check comparision with USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))
# USGS version, used for producing hexagons: 
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

setwd(localdir)

# Make TN/Overlay directory, if it doesn't already exist
system(paste('mkdir -p', file.path(localdir, "TN", "Overlay")))

TEST = F      # Change to F to run for all available months, T for only a subset of months
CHECKPLOT = F # Make plots for each stage, to make sure of spatial overlay

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


# Read in grid shapefiles and overlay in a loop ----

grids = c("TN_01dd_fishnet",
          "TN_1sqmile_hexagons")

for(g in grids){ # g = grids[1]
  
  grid = readOGR(file.path(localdir, state, "Shapefiles"), layer = g) # the grid layer
  grid <- spTransform(grid, CRS(proj.USGS))
  # grid column names
  gridcols <- names(grid)[grep("^GRID", names(grid))]
  
  FIPS_i = formatC(state.fips[state.fips$abb == state, "fips"][1], width = 2, flag = "0") # find the FIPS ID of TN
  co_i <- co[co$STATEFP == FIPS_i,] # find all counties fall in TN
  
  # Limit grid to only cells which touch a county in this state
  grid_o <- gIntersects(grid, co_i, byid = T)
  instate <- apply(grid_o, 2, function(x) any(x))
  grid <- grid[instate,]
  
  # Files -- 
  # Names of files for this state, in a directory with all clipped, buffered, unique uuid events by month.
  wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
  wazemonthfiles <- wazemonthfiles[grep("TN_\\d{4}-\\d{2}.RData$", wazemonthfiles)]
  wazemonths <- sort(unique(substr(wazemonthfiles, 4, 10)))
  
  # Use just Waze data
  months_shared <- wazemonths #sort(tn_crash_months[tn_crash_months %in% wazemonths])
  
  # Not months all are shared -- TN crash data through Sept 2018 
  #  summary(wazemonths %in% tn_crash_months)
  
  avail.cores <- parallel::detectCores()
  if(avail.cores > length(months_shared)) avail.cores = length(months_shared) # use only cores necessary
  cl <- makeCluster(avail.cores) 
  registerDoParallel(cl)
  
  starttime_state <- Sys.time()
  
  # Start parallel loop over yearmonths within state ----
  foreach(mo = months_shared, .packages = c("dplyr", "tidyr","sp","rgdal","scales")) %dopar% {
    # mo = "2018-03" # mo = "2017-04"
    
    # Waze: Comes from aggregated monthly Waze events, clipped to a 0.5 mile buffer around state, for each month.  All the waze files share the same object name, mb
    load(file.path(wazedir, wazemonthfiles[grep(mo, wazemonthfiles)]))
    if(class(mb)=="data.frame"){
      mb <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                                    proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure Waze data is a SPDF
      
      mb <- spTransform(mb, CRS(proj.USGS))
    }
    
    # Get time and last.pull.time as POSIXct
    if(!("POSIXct" %in% class(mb$time))){
      mbt <- as.character(mb$time)
      mbt.last <- as.character(mb$last.pull.time)
      mbtz <- substr(mbt, 21, 23) 
      
      new.mb <- vector()
      
      for(tz in unique(mbtz)){ # tz = "CST"
        mbx.tz = mb[mbtz == tz,] # subset data frame mb to just rows with this time zone
        mbx.tz$time = as.POSIXct(mbt[mbtz == tz], tz = tz)
        mbx.tz$last.pull.time = as.POSIXct(mbt.last[mbtz == tz], tz = tz)
        new.mb = rbind(new.mb, mbx.tz@data)
      }
      mb = new.mb; rm(new.mb, mbt, mbt.last, mbtz)  
      mb <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                                  proj4string = CRS("+proj=longlat +datum=WGS84")) # convert back to spatial points data frame
      
      mb <- spTransform(mb, CRS(proj.USGS))
    }
    
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Overlay points in polygons
    # Use over() from sp to join these to the census polygon. Points in Polygons, pip
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Urban area overlay ----
    
    waze_ua_pip <- over(mb, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in mb (Waze events data). UATYPE10 is Census 2010 Urban Type, U = Urban Area, C = Urban Cluster.
    table(ua$UATYP10) # have C and U, no R as named type
    # C    U 
    # 3104  497 
    mb@data <- data.frame(mb@data, waze_ua_pip)
    names(mb@data)[(length(mb@data)-1):length(mb@data)] <- c("Waze_UA_Name", "Waze_UA_Type") # Rename the two added columns
    
    # <><><><><><><><><><><><><><><><><><><><>
    # Grid overlay ----
    
    waze_hex_pip <- over(mb, grid[,gridcols]) # Match hexagon names to each row in mb. 
    # add these columns to the data frame
    mb@data <- data.frame(mb@data, waze_hex_pip)
    
    # Check:
    if(CHECKPLOT){ 
      jpeg(file = file.path(localdir, state, paste0("Figures/Checking_TN_Waze_UAoverlay_", g, "_", mo, ".jpg")), width = 500, height = 500) 
      plot(mb, main = paste("Pre-linking TN and Waze", mo))
      #points(tn_crash, col = alpha("red", 0.2))
      plot(grid, add = T, col = alpha("grey80", 0.1))
      dev.off()
    }
    # <><><><><><><><><><><><><><><><><><><><>
    # Merge and save TN-Waze
    # <><><><><><><><><><><><><><><><><><><><>
    
    # Save Waze data as dataframe
    waze.df <- mb@data 
    # names(waze.df)
    
    # save the merged file to local output directory, move to S3 after
    fn = paste0("grid.overlay.waze.tn.", g, "_", mo, ".RData")
    
    save(list=c("waze.df"), file = file.path(localdir, "TN", "Overlay", fn))
    
    system(paste("aws s3 cp",
                 file.path(localdir, "TN", "Overlay", fn),
                 file.path(teambucket, "TN", "Overlay", fn)))
    
    if(CHECKPLOT) { 
      lwe.sp <- SpatialPoints(link.waze.tn[!is.na(link.waze.tn$lon) | !is.na(link.waze.tn$lat),c("lon", "lat")],
                                   proj4string = CRS("+proj=longlat +datum=WGS84")) 
      lwe.sp <-spTransform(lwe.sp, CRS(proj.USGS))
      jpeg(file = file.path(localdir, state, paste0("Figures/Checking2_TN_Waze_UAoverlay_", g, "_", mo, ".jpg")), width = 500, height = 500) 
      plot(co_i, main = paste("Linked TN-Waze", mo))
      plot(lwe.sp, col = alpha("blue", 0.3), add=T, pch = "+")
      dev.off()
    }  
  } # End month loop
  
  timediff <- Sys.time() - starttime_state
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to overlay TN", g, "\n\n")

  stopCluster(cl); gc()
  
} #End grid loop

