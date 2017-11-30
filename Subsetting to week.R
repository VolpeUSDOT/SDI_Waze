# Creating weekly Waze data from clipped, trimmed monthly data

# !!! Note: used pubMillis from the aggregated data for this. Now I see that that is the start time of the incident, while months were aggregated based on last pull. So there can be many weeks from one month. This will be fixed after revisiting the initial aggregation code. 

# <><><><><><><><><><><><><><><><><><><><>
# <><><><><><><><><><><><><><><><><><><><>

#### Start with RData formatted input

inputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/By month_MD_only"

outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/weekly_MD"

setwd(inputdir)

monthlyfiles <- dir()[grep("RData", dir())]

for(i in monthlyfiles){ # i = "MD__2017-07.RData"
  
  starttime <- Sys.time()
  
  load(i)
  
  # Make sure any unnecessary columns are trimmed out.
  # Some had unusused row.names column in here as well as unused pubMillis_date: !!!! check to see what these actually are. Want to keep initial time, last pull time, and duration.
  dropcols <- match(c("V1","X", "X.1", "X.2", "X.3", "pubMillis_date", "date", "filename", "country"), names(d))
  d1 <- d[,is.na(match(1:ncol(d), dropcols))]

  # First iteration: manual 
  # # Pick out one week of data for Erika: 2017-07-16 to 2017-07-22
  # starttime <- "2017-07-16 00:00:00 EDT"
  # stoptime <-  "2017-07-22 23:59:59 EDT"
  # 
  # dx <- d1[d1$time > starttime & d1$time < stoptime,]

  # Now: find week of year from POSIX datetim
  d1$week <- format(d1$time, "%W")
  
  # Find unique weeks within this month. Note that weeks spanning months will require rbinding after the fact.
  weeks <- formatC(sort(unique(d1$week)), width = 2, flag = 0) # ensure two-digit week with leading 0 for first 9 weeks of year
  
  iname <- sub("\\.RData", "", i)
  
  for(j in weeks){

    dx <- d1[d1$week == j,]

    save(list = "dx", file = file.path(outputdir, paste(iname, "_wk", j, ".RData", sep = "")))
    
    write.csv(dx@data, 
              file = file.path(outputdir, paste(iname, "_wk", j, ".csv", sep = "")),
              row.names = F)

    }

  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")

}