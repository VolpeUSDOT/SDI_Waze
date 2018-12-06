# Query alert, jam and jam_point_sequence for Bellevue case study
# Prep in case we want these data. Built off of non-recurring congestion work for innovation challenge


# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) 

user <- paste0( "/home/", system("whoami", intern = TRUE)) 
localdir <- paste0(user, "/workingdata/") # full path for readOGR
setwd(localdir)

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

library(tidyverse)
library(lubridate)

source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))

# 1. Query alerts ----

startday = '2018-01-01'
endday   = '2018-11-30'

# Bounding box which roughly encompasses all of Bellevue
latrange = c(47.0,  48.0) 
lonrange = c(-122.0, -123)

alert_query <- paste0("SELECT DISTINCT alert_uuid, alert_type, sub_type, street, city, state, magvar, report_description, location_lat, location_lon, jam_uuid, start_pub_utc_timestamp, pub_utc_timestamp, road_type from dw_waze.alert
WHERE state='WA'
  AND pub_utc_timestamp BETWEEN to_timestamp('",startday, " 00:00:00','YYYY-MM-DD HH24:MI:SS') 
  AND     to_timestamp('", endday," 23:59:59','YYYY-MM-DD HH24:MI:SS')
  AND location_lat BETWEEN ", latrange[1], " AND ", latrange[2], " AND location_lon BETWEEN ", lonrange[1], " AND ", lonrange[2],";")

results <- dbGetQuery(conn, alert_query) 

cat(format(nrow(results), big.mark = ","), "observations \n")

fn = paste0("Bellevue_Raw_events_", startday, "_to_", endday,".csv")

write.csv(results, file = file.path(localdir, "WA", fn))

# Copy to S3
system(paste("aws s3 cp",
             file.path(localdir, "WA", fn),
             file.path(teambucket, "WA", fn)))

# 3.125 M rows

# 2. Query jams ----

# MA notes
# Looping over one month at time, 1 - 1.6 GB each as .csv. Also saving as .RData for easier further processings.
# Each month 5-7 million rows, takes ~ 7 min to query three months and upload.
# Save an object with all unique jam_id values 

months = 1:11

yearmonths = paste(2018, formatC(months, width = 2, flag = "0"), sep="-")

yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

jam_ids <- vector()

starttime <- Sys.time()

for(mo in 1:length(months)){ # mo = 1

  jam_query <- paste0("SELECT * from dw_waze.jam WHERE state ='WA' AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[mo], " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[mo], " 23:59:59','YYYY-MM-DD HH24:MI:SS');")

  results <- dbGetQuery(conn, jam_query) 
  
  cat(format(nrow(results), big.mark = ","), "observations \n")
  timediff <- Sys.time() - starttime
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to query to", yearmonths.end[mo], "\n\n")
  
  fn = paste0("WA_jams_", yearmonths.1[mo], "_to_", yearmonths.end[mo],".csv")
  
  write.csv(results, file = file.path(localdir, "WA", fn), row.names=F)

  fn.R = paste0("WA_jams_", yearmonths.1[mo], "_to_", yearmonths.end[mo],".RData")
  
  save("results", file = file.path(localdir, "WA", fn.R))
  
  # Copy to S3
  system(paste("aws s3 cp",
               file.path(localdir, "WA", fn),
               file.path(teambucket, "WA", fn)))
  
  # Save jam ids
  jam_ids <- c(jam_ids, unique(results$jam_uuid))
}

jam_ids <- unique(jam_ids)

save('jam_ids', file = file.path(localdir, "WA", "Jam_ID.RData"))

system(paste("aws s3 cp",
             file.path(localdir, "WA", "Jam_ID.RData"),
             file.path(teambucket, "WA", "Jam_ID.RData")))

# 3. Query jam_point_sequence ----
# Only have jam uuid and lat long. So can match to jam_ids from jam table, and also restrict by lat long.

load(file.path(localdir, 'WA', 'Jam_ID.RData'))

# jam_id	        varchar(50)	
# location_x    	varchar(50)
# location_y    	varchar(50)	
# sequence_order	smallint	


# 50 chunk solution - was 13 minutes, result is 284 Mb in R, 329 Mb as .csv, 466 Mb as .RData.
# Now 51 minutes

jam_ids_cut <- cut(1:length(jam_ids), breaks = 50)

results_all <- vector()
jamcount = 0
count = 0

starttime <- Sys.time()

for(i in levels(jam_ids_cut)){

  jam_point_query <- paste0("SELECT * from dw_waze.jam_point_sequence
                            WHERE location_y BETWEEN ", latrange[1], "AND ", latrange[2],
                            "AND location_x BETWEEN ", lonrange[1], "AND ", lonrange[2],
                            " AND jam_id IN ('",  
                            paste(jam_ids[jam_ids_cut==i], collapse = "','")
                            ,"');")
  
  results <- dbGetQuery(conn, jam_point_query) 
  
  cat(format(nrow(results), big.mark = ","), "observations \n")
  timediff <- Sys.time() - starttime
  
  jamcount = jamcount + length(jam_ids[jam_ids_cut==i])
  count = count + 1
  
  cat(round(timediff, 2), attr(timediff, "units"), "elapsed to query", format(jamcount, big.mark=","), "jams, this is group", count, "of", length(levels(jam_ids_cut)),"\n\n")

  results_all <- rbind(results_all, results)
  
}

timediff <- Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, "units"), "elapsed to query all jams\n\n")

# Save to S3
fn = paste0("WA_jam_point_sequence.csv")

write.csv(results_all, file = file.path(localdir, "WA", fn), row.names=F)

fn.R = paste0("WA_jam_point_sequence.RData")

save("results_all", file = file.path(localdir, "WA", fn.R))

system(paste("aws s3 cp",
             file.path(localdir, "WA", fn),
             file.path(teambucket, "WA", fn)))


# 4. Reprocess jams ----

#  Given that now we have jams located in time (from jam table, step 2) and located in both space and time (from jam_point_sequence, queried with step 2. jam IDs), we can go back to the jam table and limit by space.

load(file.path(localdir, "WA", "WA_jam_point_sequence.RData")) # called results_all. Remove after grabbing IDs to save RAM

jam_id_space_time <- unique(results_all$jam_id); rm(results_all)

cat(format(length(jam_id_space_time), big.mark=","), "unique jam IDs by the space and time filters")

# 5,187,471 M unique jam IDs now

# load the jam data, in month chunks

jam_data_files <- dir(file.path(localdir, "WA"))[grep("WA_jams_2018", dir(file.path(localdir, "WA")))]
jam_data_files <- jam_data_files[grep("rdata$", tolower(jam_data_files))]

all_jams <- vector()

for(i in jam_data_files){
  load(file.path(localdir, "WA", i)) # called results, each about 1 Gb
  
  # filter by jam_id_space_time here
  r2 <- results %>% 
    filter(jam_uuid %in% jam_id_space_time) %>%
    select(id, jam_type, road_type, street, city, state, speed,
           start_node, end_node, level, jam_length, delay, turn_line, turn_type, blocking_alert_uuid,
           pub_utc_timestamp)
  
  all_jams <- rbind(all_jams, r2)
  
  rm(results, r2)
  }

format(object.size(all_jams), "Gb") # all jams now 2.8 Gb 
format(nrow(all_jams), big.mark = ",") # 23,777,516 total observations
format(length(unique(all_jams$id)), big.mark = ",") # 5,187,471 unique jams


length(unique(all_jams$id)) == length(jam_id_space_time) # TRUE

# Save to S3
fn = paste0("WA_jams_2018-01-01_to_2018-11-30.csv")

write.csv(all_jams, file = file.path(localdir, "WA", fn), row.names=F)

fn.R = paste0("WA_jams_2018-01-01_to_2018-11-30.RData")

save("all_jams", file = file.path(localdir, "WA", fn.R))

system(paste("aws s3 cp",
             file.path(localdir, "WA", fn),
             file.path(teambucket, "WA", fn)))


system(paste("aws s3 cp",
             file.path(localdir, "WA", fn.R),
             file.path(teambucket, "WA", fn.R)))
