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
  writeLines("", paste0("waze_waze_log_", Sys.Date(), ".txt")) # to store messages
  
  # Start of %dopar% loop
  linktable <- foreach(i=1:nrow(accfile), .combine = rbind, .packages = "sp") %dopar% {
    
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
    
    if(i %% 5000 == 0) {
      timediff <- round(Sys.time()-starttime, 2)
      cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n",
      "approx", round(as.numeric(timediff)/i * (nrow(accfile)-i), 2), attr(timediff, "units"), "remaining \n",
        rep("<>",20), "\n\n",
       file = paste0("waze_waze_log_", Sys.Date(), ".txt"), append = T) }

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

# Function to show obejcts in the memory, orderd by size. Useful for cleaning up the workspace to free up RAM
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

# Print diagnotstics from a confusion matrix
# given a 2x2 table where columns are observed negative and postive, and rows are predicted negative and positive:
# |: ---------------------------- Observed ------:|
# |: ----------------------:|:Negative:|:Positive:|
# |: Predicted  :|:Negative:|   TN     |    FN    |
# |:            :|:Positive:|   FP     |    TP    |

bin.mod.diagnostics <- function(predtab){

  accuracy = (predtab[1,1] + predtab[2,2] )/ sum(predtab) # true positives and true negatives divided by all observations
  precision = (predtab[2,2] )/ sum(predtab[2,]) # true positives divided by all predicted positives
  recall = (predtab[2,2] )/ sum(predtab[,2]) # true positives divided by all observed positives
  false.positive.rate = (predtab[2,1] )/ sum(predtab[,1]) # false positives divided by all observed negatives

  round(t(data.frame(accuracy, precision, recall, false.positive.rate)), 4)  
}


# Read in hexagonally-gridded data of a specific name, prep fields, and save it as a short-named data frame
prep.hex <- function(hexname, month, s3 = T, bucket = waze.bucket){
  # Defaults to read from S3 bucket specified with waze.bucket 
  # Specify month as two-digit character value, e.g. "04" for April
  # Specify full path in hexname, e.g. file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData"))
  
  mo = month
  if(s3) { 
    s3load(object = hexname, bucket = bucket, envir = environment())
  } else {
    load(hexname, envir = environment())
  }
  
  wte <- get(ls(envir = environment())[grep("WazeTime", ls(envir = environment()), ignore.case = T)])
  
  if(length(grep("DayOfWeek", names(wte)) > 0)){
    wte$DayOfWeek <- as.factor(wte$DayOfWeek)
  }

  if(length(grep("weekday", names(wte)) > 0)){
    wte$DayOfWeek <- as.factor(wte$weekday)
  }
  
  wte$hour <- as.numeric(wte$hour)
  
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
  
  assign(paste("w", mo, sep="."), wte, envir = globalenv()) 
  }
