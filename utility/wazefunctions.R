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
    
    id.accident <- rep(as.character(ei[,accidvar]), nrow(d.t)) # Waze
    id.incidents <- as.character(d.t[,incidvar]) # EDT
    
    if(i %% 50000 == 0) {
      timediff <- round(Sys.time()-starttime, 2)
      cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n",
      "approx", round(as.numeric(timediff)/i * (nrow(accfile)-i), 2), attr(timediff, "units"), "remaining \n",
        rep("<>",20), "\n\n",
       file = paste0("EDT_Waze_log_", i, "_", Sys.Date(), ".txt"), append = T) }

    data.frame(id.accident, id.incidents) # rbind this output
  } # end %dopar% loop
  
  stopCluster(cl) # stop the cluster.
  
  linktable # Give all Waze accident IDs with EDT incident IDs
  }
  
# Non-parallel
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
prep.hex <- function(hexname, state, month, bucket = teambucket){
  # Defaults to read from S3 bucket specified with teambucket 
  # Specify month as 6-digit character value, e.g. "2017-04" for April 2017
  # Specify full path in hexname, e.g. for MD weather-overlaid files: file.path(inputdir, paste0("WazeTimeEdtHexWx_", mo,".RData"))
  # system(paste0('aws s3 ls ', bucket, '/', state, '/'))
  
  mo = month
  system(paste0('aws s3 cp ', bucket, '/', state, '/', hexname, ' ~/workingdata/', state))
  load(file.path("~/workingdata", state, hexname))

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
  
  if(state == "TN"){
    # Going to binary for all Waze buffer match, for Tennessee
    
    wte$MatchTN_buffer <- wte$nMatchTN_buffer
    wte$MatchTN_buffer[wte$MatchTN_buffer > 0] = 1 
    wte$MatchTN_buffer <- as.factor(wte$MatchTN_buffer)
    wte$MatchTN_buffer_Acc <- wte$nMatchTN_buffer_Acc
    wte$MatchTN_buffer_Acc[wte$MatchTN_buffer_Acc > 0] = 1 
    wte$MatchTN_buffer_Acc <- as.factor(wte$MatchTN_buffer_Acc)
    
    # Going to binary for all Waze Accident buffer match:
    
    wte$MatchTN_buffer_Acc <- wte$nMatchTN_buffer_Acc
    wte$MatchTN_buffer_Acc[wte$MatchTN_buffer_Acc > 0] = 1 
    wte$MatchTN_buffer_Acc <- as.factor(wte$MatchTN_buffer_Acc)
  } else {
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
  }  
  
  # Omit grid cell x day combinations which are outside of this particular month (Waze road closures)
  yr = substr(mo, 1, 4)
  month.2 = substr(mo, 6, 7)
  yrday = strptime(paste(yr, formatC(wte$day, width =3, flag = 0), sep="-"), "%Y-%j")
  month.1 = format(yrday, "%m")
  wte = wte[month.1 %in% month.2,]
  
  mo <- sub("-", "_", mo) # change e.g. from 2017-04 to 2017_04 for R object naming
  
  assign(paste("w", mo, sep="."), wte, envir = globalenv()) 
  }

# Append jobs, road class, or future other gridded data.
# This version will be superceded by 'append.hex2', but keeping here for compatibility with previous scripts

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

# Another version of append.hex, updated for SDC and the new supplemental data, including VMT.

append.hex2 <- function(hexname, data.to.add, state, na.action = c("omit", "keep", "fill0"), IDprefix = "1"){
  # hexname: string of data frame names in working memory like "w.04"
  # data.to.add: string of unzipped file set in the localdir, including sub-directory path and state name, like "FARS/CT/FARS_CT_2015_2016_sum_fclass"
  # na.action: what to do if missing any values; applies this action across the whole data frame, not just the appended data
  if(missing(na.action)) { na.action = "omit" } # set default value
  na.action <- match.arg(na.action) # match partially entered values
  
  w <- get(hexname)
  
  # Check to see if this shapefile has been read in already. For FARS multipoint data, simply read DBF file
  if(!exists(data.to.add)){
    if(length(grep("FARS", data.to.add)) > 0){
      assign(data.to.add, foreign::read.dbf(file = file.path(localdir, "FARS", state, paste0(data.to.add, ".dbf"))), envir = globalenv())
    }
    if(length(grep("aadt", data.to.add)) > 0){
      assign(data.to.add, read.csv(file = file.path(localdir, "AADT", paste0(data.to.add, ".txt"))),
             envir = globalenv())
      
      } 
    
    if(length(grep("bg_wac", data.to.add)) > 0 | length(grep("bg_rac", data.to.add)) > 0 ){
      assign(data.to.add, foreign::read.dbf(file = file.path(localdir, "LODES_LEHD", state, paste0(data.to.add, ".dbf"))),
             envir = globalenv())
      
    } 
    # { # Build this out lodes, aadt
    #   assign(data.to.add, rgdal::readOGR(localdir, layer = data.to.add), envir = globalenv())
    # }
  }
  
  dd <- get(data.to.add)
  
  if(length(grep("FARS", data.to.add)) > 0){
    dd <- dd %>% 
      group_by(GRID_ID) %>%
      summarise(CRASH_SUM = sum(CRASH_SUM),
                FATALS_SUM = sum(FATALS_SUM))
  }
  
  # Expand VMT from month / day of week / hour to day of year / hour of day, for each grid cell
  
  if(length(grep("max_aadt_by_grid", data.to.add)) > 0){
    # Check to see if these processing steps have been done yet; don't need to re-do for each month.
    prepname = paste("Prepared", data.to.add, sep="_")
    
    if(!exists(prepname)) {
    
    cat("Preparing", data.to.add, "\n")
    # Spread for multiple columns by road functional class
    
    # dd.vol <- dd %>% 
    #   group_by(GRID_ID, month, dayofweek, hour) %>%
    #   tidyr::spread(key = F_SYSTEM_VN, value = volume, fill = 0, sep = "_")
    
    # Rows ahave to be uniquely described by grid id, month, dayofweek, hour, and road class
    # summary(duplicated(with(dd, paste(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN))))
    dd <- dd[!duplicated(with(dd, paste(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN))),]
      
    dd.summax <- dd %>%
      group_by(GRID_ID, month, dayofweek, hour) %>%
      select(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN, SUM_MAX_AADT_VN) %>%
      tidyr::spread(key = F_SYSTEM_VN, value = SUM_MAX_AADT_VN, fill = 0, sep = "_")

    dd.hourmax <- dd %>%
      group_by(GRID_ID, month, dayofweek, hour) %>%
      select(GRID_ID, month, dayofweek, hour, F_SYSTEM_VN, HOURLY_MAX_AADT) %>%
      tidyr::spread(key = F_SYSTEM_VN, value = HOURLY_MAX_AADT, fill = 0, sep = "_")

    # Rename columns and join
    dd.summax <- dd.summax %>% 
      rename(SUM_MAX_AADT_1 = F_SYSTEM_VN_1,
             SUM_MAX_AADT_2 = F_SYSTEM_VN_2,
             SUM_MAX_AADT_3 = F_SYSTEM_VN_3,
             SUM_MAX_AADT_4 = F_SYSTEM_VN_4,
             SUM_MAX_AADT_5 = F_SYSTEM_VN_5)

    dd.hourmax <- dd.hourmax %>% 
      rename(HOURLY_MAX_AADT_1 = F_SYSTEM_VN_1,
             HOURLY_MAX_AADT_2 = F_SYSTEM_VN_2,
             HOURLY_MAX_AADT_3 = F_SYSTEM_VN_3,
             HOURLY_MAX_AADT_4 = F_SYSTEM_VN_4,
             HOURLY_MAX_AADT_5 = F_SYSTEM_VN_5)
    
    dd <- full_join(dd.summax, dd.hourmax, by = c("GRID_ID", "month", "dayofweek", "hour"))
    
    dd$vmt_time = with(dd, paste(month, dayofweek, hour, sep="_"))
    
    # Save this to global environment for other months to use
    assign(prepname, dd, envir = globalenv())
    
      } else {
      
      # Create vectors in w for month of year, day of week, hour of day in w. This is used for joining on the grid ID and time factors
      
     
      dd = get(prepname, envir = globalenv()) # Use the already prepared data if present in the working enviroment
      
  }
    
    # Extract year from file name
    yr = substr(hexname, 3, 6)
    date = strptime(paste(yr, w$day, sep = "-"), "%Y-%j")
    mo = as.numeric(format(date, "%m"))
    dow = lubridate::wday(date) # 7  = saturday, 1 = sunday.
    w$vmt_time = paste(mo, dow, w$hour, sep="_")
    
  }
  
  if(length(grep("routes_AADT_total_sum", data.to.add)) > 0){
    dd <- dd@data
  }
  
  if(length(grep("routes_sum", data.to.add)) > 0){
    dd <- dd@data %>% 
      group_by(GRID_ID) %>%
      tidyr::spread(key = F_SYSTEM_V, value = SUM_miles, fill = 0, sep = "_")
  }
  

  if(length(grep("bg_rac_", data.to.add)) > 0){
    
    #dd <- dd@data 
    dd <- dd[c("GRID_ID", 
               "SUM_C000",                                                            # Total jobs
               #               "SUM_CA01", "SUM_CA02", "SUM_CA03",                                    # By age category
               "SUM_CE01", "SUM_CE02", "SUM_CE03"                                    # By earnings
               #               ,"SUM_CD01", "SUM_CD02", "SUM_CD03", "SUM_CD04",                        # By educational attainment
               #               "SUM_CS01", "SUM_CS02"                                                 # By sex
    )]
    names(dd)[2:length(names(dd))] <- paste("RAC", names(dd)[2:length(names(dd))], sep = "_")
    
  }
  
  if(length(grep("bg_wac_", data.to.add)) > 0){
    
    #dd <- dd 
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
  
  # End data type if statements, now merge with w data frame of Waze-EDT data
  # join ----
  
  # Match with new grid ID 
  if(sum(dd$GRID_ID %in% w$GRID_ID) == 0 & substr(dd$GRID_ID[1], 1, 1)=="A"){
    cat("Appending", IDprefix, "to GRID_ID \n")
    dd$GRID_ID <- paste0(IDprefix, dd$GRID_ID)
  }
  
  # For Maryland! Match with old grid ID
  if(sum(dd$GRID_ID %in% w$GRID_ID) == 0 & substr(w$GRID_ID[1], 1, 1)=="A"){
    cat("Appending", IDprefix, "to w$GRID_ID \n")
    w$GRID_ID <- paste0(IDprefix, w$GRID_ID)
  }
  
  dd$GRID_ID <- as.character(dd$GRID_ID)
  
  if(length(grep("max_aadt_by_grid", data.to.add)) > 0){
    
    # summary(unique(w$GRID_ID) %in% unique(dd$GRID_ID)) # should be all T, but there are 1620 F in April MD? 
    # summary(w$vmt_time %in% dd$vmt_time) # all T
    
    # month / day of week / hour of day is duplicated within GRID ID in w, which is expected
    # summary(duplicated(paste(w$GRID_ID, w$vmt_time)))
    # summary(duplicated(paste(dd$GRID_ID, dd$vmt_time))) # should be all F
    # summary(duplicated(paste(dd$GRID_ID, dd$month, dd$dayofweek, dd$hour))) 
    
    w2 <- left_join(w, dd %>% ungroup() %>% select(-month, -dayofweek, -hour), by = c("GRID_ID", "vmt_time")) 
  
    # check:
    # 1Z-48, day 229 (August, dayofweek = 5) hour 16: max hourly 4 = 0.809305; 5 = 0.066670
    # format(strptime("2017-229", "%Y-%j"), "%m") 
    # lubridate::wday(strptime("2017-229", "%Y-%j")) 
    # dd[dd$GRID_ID == "1Z-48" & dd$month == 8 & dd$dayofweek == 5 & dd$hour == 16,]
    
  } else {    
    w2 <- left_join(w, dd, by = "GRID_ID")
  }
  
  # Consider assigning 0 to NA values after joining; e.g. no road info available, give 0 miles
  if(na.action == "fill0") { w2[is.na(w2)] = 0 }
  if(na.action == "omit") { w2 = w2[complete.cases(w2),] }
  
  assign(hexname, w2, envir = globalenv()) 
  
}
