# Functions for working with Waze and EDT data

# Make a link table to match events
# making more generic: accident file (usually EDT) and inicident file (usually Waze)
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.

makelink <- function(accfile = edt.april, incfile = waze.april,
                     acctimevar = "CrashDate_Local",
                     inctimevar1 = "time",
                     inctimevar2 = "last.pull.time",
                     accidvar = "ID",
                     incidvar = "uuid"
                     ){
  library(foreach) 
  library(doParallel) # includes iterators and parallel
  
  cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
  registerDoParallel(cl)
  
  linktable <- vector()
  
  starttime <- Sys.time()
  writeLines("", paste0("EDT_Waze_log_", i, "_", Sys.Date(), ".txt")) # to store messages by state i
  
  # Start of %dopar% loop
  linktable <- foreach(i=1:nrow(accfile), .combine = rbind, .packages = "sp") %dopar% {
    
    ei = accfile[i,]
    
    dist.i <- spDists(ei, incfile)*0.0006213712 # spDists gives units in m for projected data, convert to miles
    dist.i.5 <- which(dist.i <= 0.5)
    
    # Spatially matching
    d.sp <- incfile[dist.i.5,]
    
    # Temporally matching
    # Match between the first reported time and last pull time of the Waze event. Last pull time is after the earliest time of EDT, and first reported time is earlier than the latest time of EDT
    if(class(ei)=="SpatialPointsDataFrame") { ei <- as.data.frame(ei) }
    if(class(d.sp)=="SpatialPointsDataFrame") { d.sp <- as.data.frame(d.sp) }
    
    # ei: EDT events. We want to look from the time of EDT event -60 minutes to EDT event +60 minutes, and find Waze events in this window
    # d.sp: Waze events. inctimevar2 is the *end* of the event and inctimevar1 is the *start* of the event. We look to see if the end of the event is greater than EDT event -60 minutes and see if the start of the Waze event is less than the EDT event +60 minutes.
    d.t <- d.sp[d.sp[,inctimevar2] >= ei[,acctimevar]-60*60 & d.sp[,inctimevar1] <= ei[,acctimevar]+60*60,] 
    
    id.accident <- rep(as.character(ei[,accidvar]), nrow(d.t))
    id.incidents <- as.character(d.t[,incidvar])
    
    if(i %% 50000 == 0) {
      timediff <- round(Sys.time()-starttime, 2)
      cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n",
      "approx", round(as.numeric(timediff)/i * (nrow(accfile)-i), 2), attr(timediff, "units"), "remaining \n",
        rep("<>",20), "\n\n",
       file = paste0("EDT_Waze_log_", i, "_", Sys.Date(), ".txt"), append = T) }

    data.frame(id.accident, id.incidents) # rbind this output
  } # end %dopar% loop
  
  stopCluster(cl) # stop the cluster.
  
  linktable
  }
  

makelink.nonpar <- function(accfile = edt.april, incfile = waze.april,
                       acctimevar = "CrashDate_Local",
                       inctimevar1 = "time",
                       inctimevar2 = "last.pull.time",
                       accidvar = "ID",
                       incidvar = "uuid"
                       ){
    
    linktable <- vector()
  
    # keeping non-parallized version here for reference
    for(i in 1:nrow(accfile)){ # i=which(edt$ID == "2023680")
      ei = accfile[i,]
      
      dist.i <- spDists(ei, incfile, longlat = T)*0.6213712 # spDists gives units in km, convert to miles
      dist.i.5 <- which(dist.i <= 0.5)
      
      # Spatially matching
      d.sp <- incfile[dist.i.5,]
      
      # Temporally matching
      # Match between the first reported time and last pull time of the Waze event. Last pull time is after the earliest time of EDT, and first reported time is earlier than the latest time of EDT
      if(class(ei)=="SpatialPointsDataFrame") { ei <- as.data.frame(ei) }
      if(class(d.sp)=="SpatialPointsDataFrame") { d.sp <- as.data.frame(d.sp) }
      
      
      d.t <- d.sp[d.sp[,inctimevar2] >= ei[,acctimevar]-60*60 & d.sp[,inctimevar1] <= ei[,acctimevar]+60*60,] 
      
      id.accident <- rep(as.character(ei[,accidvar]), nrow(d.t))
      id.incidents <- as.character(d.t[,incidvar])
      
      linktable <- rbind(linktable, data.frame(id.accident, id.incidents))
      
      if(i %% 1000 == 0) {
        timediff <- round(Sys.time()-starttime, 2)
        cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
      }
    } # end loop
    
    
    linktable
  }


# moving files from a temporary directory on local machine to shared drive. 
# Files are removed from the local machine by this process.
movefiles <- function(filelist, temp = outdir, wazedir){
  
  # Check to make sure the from and to directories are accessible
  if(!dir.exists(wazedir)) stop("Destination output directory does not exist or shared drive is not connected")
  if(length(filelist) < 1) stop("No files selected to move, check filelist argument")
  if(length(dir(temp)[grep(filelist[1], dir(temp))]) < 1) stop("Selected files not found in the temporary output directory")
    
  if(.Platform$OS.type == "windows"){
    # Fix path separators for Windows / R 
    temp <- gsub("\\\\", "/", temp)
    temp <- gsub("C:/U", "C://U", temp)
  }
  
  for(i in filelist){
      
    # Encase the destination path in quotes, because of spaces in path name
    system(paste0("mv ", file.path(temp, i), ' \"', file.path(wazedir, i), '\"'))
    
  }
}

# Function to return the most frequent value for character vectors.
# Breaks ties by taking the first value
ModeString <- function(x) {
  ux <- unique(x)
  
  # One unique value: return that value
  if(length(ux)==1) { 
    return(ux)
  } else {
    
    # Multiple values, no duplicates: return first one
    if(!anyDuplicated(x)) {
      return(ux[1])
    } else {
      
      # Multiple values, one category more frequent: return first most frequent
      tbl <-   tabulate(match(x, ux))
      return(ux[tbl==max(tbl)][1])
    }
  }
}

# Function for extracting time from file names
gettime <- function(x, tz = "America/New_York"){
  d <- substr(x, 5, 14)
  t <- substr(x, 16, 23)
  dt <- strptime(paste(d, t), "%Y-%m-%d %H-%M-%S", tz = tz)
}

# Function to show objects in the memory, orderd by size. Useful for cleaning up the workspace to free up RAM
showobjsize <- function(units = "Mb", limit = 100) {
  sizes <- vector()
  for (thing in ls(envir = .GlobalEnv)) { 
    sz <- format(object.size(get(thing)), 
           units = units,
           digits = 2)
    sizes <- rbind(sizes, data.frame(obj = thing, 
                                     size = as.numeric(unlist(lapply(strsplit(sz, " "), function(x) x[[1]][1])))
                                     ))
  }
  printlim <- ifelse(nrow(sizes) >= limit, limit, nrow(sizes))
  message("Object size in ", units)
  print(sizes[order(sizes$size, decreasing = T)[1:printlim],])
}

# Print diagnotstics from a confusion matrix.
# RF function give observed as rows, predicted as columns. More common in observed as columns, predicted as rows. Here following caret::confusionMatrix
# given a 2x2 table where columns are observed postive, and rows are predicted negative and positive:
# |: ---------------------------- Observed ------:|
# |: ----------------------:|:Positive:|:Negative:|
# |: Predicted  :|:Positive:|   TP     |    FP    |
# |:            :|:Negative:|   FN     |    TN    |

bin.mod.diagnostics <- function(predtab){

  accuracy = (predtab[1,1] + predtab[2,2] )/ sum(predtab) # true positives and true negatives divided by all observations
  precision = (predtab[1,1] )/ sum(predtab[1,]) # true positives divided by all predicted positives
  recall = (predtab[1,1] )/ sum(predtab[,1]) # true positives divided by all observed positives
  false.positive.rate = (predtab[1,2] )/ sum(predtab[,2]) # false positives divided by all observed negatives

  round(t(data.frame(accuracy, precision, recall, false.positive.rate)), 4)  
}


# Read in hexagonally-gridded data of a specific name, prep fields, and save it as a short-named data frame
prep.hex <- function(hexname, state, month, s3 = T, bucket = teambucket){
  # Defaults to read from S3 bucket specified with teambucket 
  # Specify month as 6-digit character value, e.g. "2017-04" for April 2017
  # Specify full path in hexname, e.g. for MD weather-overlaid files: file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData"))
  
  mo = month
  if(s3) {
    system(paste0('aws s3 cp ', bucket, '/', state, '/', hexname, ' ~/workingdata'))
    load(file.path("~/workingdata", hexname))
  } else {
    load(hexname, envir = environment())
  }
  
  wte <- get(ls(envir = environment())[grep("WazeTime", ls(envir = environment()), ignore.case = T)])
  
  class(wte) <- "data.frame" 
  # if(length(grep("DayOfWeek", names(wte)) > 0)){
  #   wte$DayOfWeek <- as.factor(wte$DayOfWeek)
  # }

  if(length(grep("weekday", names(wte)) > 0)){
    wte$DayOfWeek <- as.factor(wte$weekday)
  }
  
  wte$hextime <- as.character(wte$hextime)
  wte$hour <- as.numeric(wte$hour)
  
  # Set NA values in wx (reflectivity) to zero
  if(length(grep("wx", names(wte)) > 0)){
    wte$wx[is.na(wte$wx)] = 0
  }
  
  # Going to binary for all Waze buffer match:
  
  wte$MatchEDT_buffer <- wte$nMatchEDT_buffer
  wte$MatchEDT_buffer[wte$MatchEDT_buffer > 0] = 1 
  wte$MatchEDT_buffer <- as.factor(wte$MatchEDT_buffer)
  wte$MatchEDT_buffer_Acc <- wte$nMatchEDT_buffer_Acc
  wte$MatchEDT_buffer_Acc[wte$MatchEDT_buffer_Acc > 0] = 1 
  wte$MatchEDT_buffer_Acc <- as.factor(wte$MatchEDT_buffer_Acc)
  
  # Going to binary for all Waze Accident buffer match:
  
  wte$MatchEDT_buffer_Acc <- wte$nMatchEDT_buffer_Acc
  wte$MatchEDT_buffer_Acc[wte$MatchEDT_buffer_Acc > 0] = 1 
  wte$MatchEDT_buffer_Acc <- as.factor(wte$MatchEDT_buffer_Acc)
  
  mo <- sub("-", "_", mo) # change e.g. from 2017-04 to 2017_04 for R object naming
  
  assign(paste("w", mo, sep="."), wte, envir = globalenv()) 
  }

# Append jobs, road class, or future other gridded data.
# Assumptions: 
# 1. There are already files like "w.04" representing gridded, hourly Waze-EDT data fusions in the working environment. This script appends static data to the hourly gridded data, repeating the static values for each hour.
# 2. There are paths like the S3 bucket, inputdir, localdir, all in working environment
# 3. rgdal and tidyverse libraries are loaded
# 4. The data files to add are unzipped files in the localdir (i.e., they have already been copied over from the S3 bucket, and are sitting on the instance)

append.hex <- function(hexname, data.to.add, na.action = c("omit", "keep", "fill0"), IDprefix = "1"){
  # hexname: string of data frame names in working memory like "w.04"
  # data.to.add: string of unzipped file set in the localdir, like "hexagons_1mi_routes_sum", "hexagons_1mi_bg_rac_sum", "hexagons_1mi_bg_lodes_sum"
  # na.action: what to do if missing any values; applies this action across the whole data frame, not just the appended data
  if(missing(na.action)) { na.action = "omit" } # set default value
  na.action <- match.arg(na.action) # match partially entered values
  
  w <- get(hexname)
  
  # Check to see if this shapefile has been read in already. For FARS multipoint data, simply read DBF file
  if(!exists(data.to.add)){
    if(length(grep("FARS_MD", data.to.add)) > 0){
      assign(data.to.add, foreign::read.dbf(file = file.path(localdir, paste0(data.to.add, ".dbf"))), envir = globalenv())
    } else {
    assign(data.to.add, rgdal::readOGR(localdir, layer = data.to.add), envir = globalenv())
    }
  }
  
  dd <- get(data.to.add)
  
  if(length(grep("routes_AADT_total_sum", data.to.add)) > 0){
    dd <- dd@data
  }
  
  if(length(grep("routes_sum", data.to.add)) > 0){
    dd <- dd@data %>% 
      group_by(GRID_ID) %>%
      tidyr::spread(key = F_SYSTEM_V, value = SUM_miles, fill = 0, sep = "_")
  }

  if(length(grep("FARS_MD", data.to.add)) > 0){
    dd <- dd %>% 
      group_by(GRID_ID) %>%
      summarise(CRASH_SUM = sum(CRASH_SUM),
                FATALS_SUM = sum(FATALS_SUM))
  }
  

  if(length(grep("bg_rac_", data.to.add)) > 0){
    
    dd <- dd@data 
    dd <- dd[c("GRID_ID", 
               "SUM_C000",                                                            # Total jobs
#               "SUM_CA01", "SUM_CA02", "SUM_CA03",                                    # By age category
               "SUM_CE01", "SUM_CE02", "SUM_CE03"                                    # By earnings
#               ,"SUM_CD01", "SUM_CD02", "SUM_CD03", "SUM_CD04",                        # By educational attainment
#               "SUM_CS01", "SUM_CS02"                                                 # By sex
    )]
    names(dd)[2:length(names(dd))] <- paste("RAC", names(dd)[2:length(names(dd))], sep = "_")
    
  }
  
  if(length(grep("bg_lodes_", data.to.add)) > 0){
    
    dd <- dd@data 
    dd <- dd[c("GRID_ID", 
               "SUM_C000",                                                            # Total jobs
#               "SUM_CA01", "SUM_CA02", "SUM_CA03",                                    # By age category
               "SUM_CE01", "SUM_CE02", "SUM_CE03",                                    # By earnings
#              "SUM_CD01", "SUM_CD02", "SUM_CD03", "SUM_CD04",                        # By educational attainment
               "SUM_CS01", "SUM_CS02",                                                # By sex
#             "SUM_CFA01", "SUM_CFA02" ,"SUM_CFA03" ,"SUM_CFA04", "SUM_CFA05",       # By firm age
               "SUM_CFS01", "SUM_CFS02", "SUM_CFS03", "SUM_CFS04",  "SUM_CFS05"       # By firm size
    )]
    names(dd)[2:length(names(dd))] <- paste("WAC", names(dd)[2:length(names(dd))], sep = "_")
    
  }
  
  # Match with new grid ID 
  if(sum(dd$GRID_ID %in% w$GRID_ID) == 0 & substr(dd$GRID_ID[1], 1, 1)=="A"){
    cat("Appending", IDprefix, "to GRID_ID \n")
    dd$GRID_ID <- paste0(IDprefix, dd$GRID_ID)
  }
  
  w2 <- left_join(w, dd, by = "GRID_ID")
  # Consider assigning 0 to NA values after joining; e.g. no road info available, give 0 miles
  if(na.action == "fill0") { w2[is.na(w2)] = 0 }
  if(na.action == "omit") { w2 = w2[complete.cases(w2),] }
  
  assign(hexname, w2, envir = globalenv()) 
  
}
