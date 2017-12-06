# Aggregates daily Waze files 
# This is an edited version of Lia's original 'ReduceWaze2.R' from 
# W:\SDI Pilot Projects\Waze\Working Documents\R
# edited 11/28/2017

# setup ----

library(tidyverse)

mappeddrive = "W:" # change this to match the mapped network drive on your machine (Z: in original)

setwd(file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze"))

# read file names
wazefiles <- dir()[grep("csv", dir())]

# Make aggregated daily files ----

# Function to return the most frequent value for character vectors
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

# For now: April only
wazefiles = wazefiles[substr(wazefiles, 10, 11) == "04"]

#for each file
j = 1 # start at 21 currently...

starttime <- Sys.time()

while (j <= length(wazefiles)){
  
  bydate <- read.csv(file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze/", wazefiles[j]))

  #extract date from file name
  bydate$date <- as.POSIXct(paste(substr(bydate$filename, 5, 14), " ", substr(bydate$filename, 16, 17), ":", substr(bydate$filename, 19,20), ":", substr(bydate$filename, 22, 23), sep = ""))
  
  # drop un-useful columns
  dropcols <- c("X", "country")
  bydate <- bydate[is.na(match(names(bydate), dropcols))]
  
 
  #aggregate by uuid. Now using dplyr 
  d <- as.tibble(bydate)
  ll <- d %>%
         group_by(uuid) %>%
         summarise(
                   city = ModeString(city),
                   reportRating = median(reportRating),
                   confidence = median(confidence),
                   reliability = median(reliability),
                   type = ModeString(type),
                   roadType = ModeString(as.character(roadType)),
                   magvar = max(magvar),
                   subtype = ModeString(subtype),
                   street = ModeString(street),
                   pubMillis = min(pubMillis),
                   lon = median(location.x),
                   lat = median(location.y),
                   first.file = sort(filename, decreasing = F)[1],
                   last.file = sort(filename, decreasing = T)[1],
                   nrecord = n()
                   )
  
  # inspect a particular uuid in original to test:
  # bydate[bydate$uuid=="6407a977-9d52-380e-b545-b4ef58e70337",]
  # test: xx <- tapply(bydate$roadType, bydate$uuid, ModeString); which(sapply(xx, length) > 1)
  
  # write this day's data to RData output 
  filenamesave <- sub("csv", "RData", wazefiles[j])
  
  save(list = "ll", 
       file = file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/day_MD", filenamesave))
  
  # write to csv temporarily as example
  # write.csv(ll, 
  #      file = file.path(mappeddrive,"/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/day_MD", wazefiles[j]),
  #      row.names = F)
  
  timediff <- round(Sys.time()-starttime, 2)
  cat(j, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")

  j = j+1
  }


# Compiling Monthly ----

#redo with months in a clunky way
setwd(file.path(mappeddrive, "/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/"))

#read files
wazefiles <- list.files()

#make a dataframe for each month
i=1
mar <- data.frame(matrix(ncol = 19, nrow = 0))
april <- data.frame(matrix(ncol = 19, nrow = 0))
may <- data.frame(matrix(ncol = 19, nrow = 0))
june <- data.frame(matrix(ncol = 19, nrow = 0))
july <- data.frame(matrix(ncol = 19, nrow = 0))
august <- data.frame(matrix(ncol = 19, nrow = 0))
sept <- data.frame(matrix(ncol = 19, nrow = 0))
oct <- data.frame(matrix(ncol = 19, nrow = 0))
while(i <= length(wazefiles)){
  x <- substr(wazefiles[i], 10,11)
  temp <- as.data.frame(read.csv(paste("Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/", wazefiles[i], sep = "")))
  if(x == "03"){
    mar <- rbind(mar, temp)
    i=i+1
  }else if(x == "04"){
      april <- rbind(april, temp)
      i=i+1
  }else if(x == "05"){
    may <- rbind(may, temp)
    i=i+1
  }else if(x == "06"){
    june <- rbind(june, temp)
    i=i+1
  }else if(x == "07"){
    july <- rbind(july, temp)
    i=i+1
  }else if(x == "08"){
    august <- rbind(august, temp)
    i=i+1
  }else if(x == "09"){
    sept <- rbind(sept, temp)
    i=i+1
  }else if(x == "10"){
    oct <- rbind(oct, temp)
    i=i+1
  }
}
    
#do the same processing on whole months as done on whole days. i just ran this for each month by hand without doing a loop. note the month names. 

#mar
#aggregate by uuid and the highest reliability
list_match <- aggregate(reliability ~ uuid, data = mar, FUN = max)
mar$last_data_pull <- as.POSIXct(mar$last_data_pull, tz = "America/New_York")
mar$last_pull_GMTnumeric <- as.numeric(as.POSIXct(mar$last_data_pull, tz = "GMT"))
mar$pubMillis_date <- as.POSIXct(mar$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")

#aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
#rename and remove the duplicative uuid column
list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = mar, FUN = max))
names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
list_match <- list_match[, !names(list_match) %in% c("uuid2")]

#match original data to unique uuid + max reliability data (many entries per uuid)
#remove duplicates (one entry per uuid)
#write to a csv
aggregated_by_uuid1 <- merge(mar, list_match, by = c("uuid","reliability"))
aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
remove(aggregated_by_uuid1)
write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-03.csv")

#APRIL
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = april, FUN = max)
  april$last_data_pull <- as.POSIXct(april$last_data_pull, tz = "America/New_York")
  april$last_pull_GMTnumeric <- as.numeric(as.POSIXct(april$last_data_pull, tz = "GMT"))
  april$pubMillis_date <- as.POSIXct(april$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = april, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(april, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-04.csv")
  
  #MAY
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = may, FUN = max)
  may$last_data_pull <- as.POSIXct(may$last_data_pull, tz = "America/New_York")
  may$last_pull_GMTnumeric <- as.numeric(as.POSIXct(may$last_data_pull, tz = "GMT"))
  may$pubMillis_date <- as.POSIXct(may$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = may, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(may, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-05.csv")
  
  #JUNE
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = june, FUN = max)
  june$last_data_pull <- as.POSIXct(june$last_data_pull, tz = "America/New_York")
  june$last_pull_GMTnumeric <- as.numeric(as.POSIXct(june$last_data_pull, tz = "GMT"))
  june$pubMillis_date <- as.POSIXct(june$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = june, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(june, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-06.csv")
  
  #JULY
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = july, FUN = max)
  july$last_data_pull <- as.POSIXct(july$last_data_pull, tz = "America/New_York")
  july$last_pull_GMTnumeric <- as.numeric(as.POSIXct(july$last_data_pull, tz = "GMT"))
  july$pubMillis_date <- as.POSIXct(july$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = july, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(july, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-07.csv")
  
  #AUG
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = august, FUN = max)
  august$last_data_pull <- as.POSIXct(august$last_data_pull, tz = "America/New_York")
  august$last_pull_GMTnumeric <- as.numeric(as.POSIXct(august$last_data_pull, tz = "GMT"))
  august$pubMillis_date <- as.POSIXct(august$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = august, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(august, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-08.csv")
  
  #SEPT
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = sept, FUN = max)
  sept$last_data_pull <- as.POSIXct(sept$last_data_pull, tz = "America/New_York")
  sept$last_pull_GMTnumeric <- as.numeric(as.POSIXct(sept$last_data_pull, tz = "GMT"))
  sept$pubMillis_date <- as.POSIXct(sept$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = sept, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(sept, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-09.csv")

  #OCT
  #aggregate by uuid and the highest reliability
  list_match <- aggregate(reliability ~ uuid, data = oct, FUN = max)
  oct$last_data_pull <- as.POSIXct(oct$last_data_pull, tz = "America/New_York")
  oct$last_pull_GMTnumeric <- as.numeric(as.POSIXct(oct$last_data_pull, tz = "GMT"))
  oct$pubMillis_date <- as.POSIXct(oct$pubMillis/1000, tz = "America/New_York", origin = "1970-01-01")
  
  #aggregate on uuid and the latest waze pull time and append that to the list with reliability maxes
  #rename and remove the duplicative uuid column
  list_match <- cbind(list_match, aggregate(last_pull_GMTnumeric ~ uuid, data = oct, FUN = max))
  names(list_match) <- c("uuid", "reliability", "uuid2", "last_data_pull")
  list_match <- list_match[, !names(list_match) %in% c("uuid2")]
  
  #match original data to unique uuid + max reliability data (many entries per uuid)
  #remove duplicates (one entry per uuid)
  #write to a csv
  aggregated_by_uuid1 <- merge(oct, list_match, by = c("uuid","reliability"))
  aggregated_by_uuid2 <- aggregated_by_uuid1[!duplicated(aggregated_by_uuid1$uuid),]
  aggregated_by_uuid2$duration <- aggregated_by_uuid2$last_pull_GMTnumeric - aggregated_by_uuid2$pubMillis/1000
  aggregated_by_uuid2 <- aggregated_by_uuid2[, !names(aggregated_by_uuid2) %in% c("last_data_pull.x", "last_data_pull.y")]
  remove(aggregated_by_uuid1)
  write.csv(aggregated_by_uuid2, file = "Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/MD__2017-10.csv")
  
  #add last pull date 
  j=1
  while (j <= length(listfiles)){
    temp <- read.csv(paste("Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/By month/", listfiles[j], sep=""))
    temp$last_pull_date <- as.POSIXct(temp$last_pull_GMTnumeric, tz = "America/New_York", origin = "1970-01-01")
    write.csv(temp, file = paste("Z:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/", listfiles[j], sep = ""))
    j=j+1
    + }