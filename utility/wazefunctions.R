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
                     incidvar = "uuid"){
  
  linktable <- vector()
  starttime <- Sys.time()
  
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
  for(i in filelist){
    # Fix path separators for Windows / R 
    temp <- gsub("\\\\", "/", temp)
    temp <- gsub("C:/U", "C://U", temp)
    
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
# given a 2x2 table where rows are observed negative and postive, and columns are predicted negative and positive
bin.mod.diagnostics <- function(predtab){

  accuracy = (predtab[1,1] + predtab[2,2] )/ sum(predtab) # true positives and true negatives divided by all observations
  precision = (predtab[2,2] )/ sum(predtab[,2]) # true positives divided by all predicted positives
  recall = (predtab[2,2] )/ sum(predtab[2,]) # true positives divided by all observed positives
  false.positive.rate = (predtab[2,1] )/ sum(predtab[1,]) # false positives divided by all observed negatives

  t(data.frame(accuracy, precision, recall, false.positive.rate))  
}
