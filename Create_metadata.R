mappeddrive="W:"
setwd(file.path(mappeddrive, "/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD"))
dir()
load("MD__2017-04.RData")


nam <- names(mb)

lengun <- apply(mb[,c("uuid", "city", "type", "roadType", "subtype","street")],
      2,
      function(x) length(unique(x))
      )

means <- apply(mb[,c("reportRating", "confidence", "reliability","magvar",
                    "nrecord")],
                2,
                function(x) mean(x, na.rm=T))
                
ranges <- apply(mb[,c("pubMillis","lon","lat", "nrecord", "time", "first.pull.time", "last.pull.time")],
               2,
               function(x) range(x, na.rm=T))

meanval <- vector(length = length(nam))
meanval[which(nam %in% names(means))] <- means

uniqval <- vector(length = length(nam))
uniqval[which(nam %in% names(lengun))] <- lengun

rangemin <- rangemax <- vector(length = length(nam))
rangemin[which(nam %in% colnames(ranges))] <- ranges[1,]
rangemax[which(nam %in% colnames(ranges))] <- ranges[2,]
### Describe variables

desc = c("Unique ID, aggregated to single row per uuid",
         "City name, mode of unique values within this uuid",
         "reportRating, median of all reportRatings within this uuid",
         "confidence, median value",
         "reliability, median value",
         "Type of incident, modal value (first if tie)",
         "roadType, modal value",
         "Direction of travel in degrees, maximum",
         "Subtype of incident, modal",
         "Street name, modal",
         "Initial report time, median",
         "longitude, median",
         "latitude, median",
         "Name of file this uuid first appears in",
         "Name of file this uuid last appears in",
         "Number of records for this uuid",
         "POSIX formatted time for initial report time",
         "Formatted time for first file this uuid appears in",
         "Formatted time for last file this uuid appears in"
         )


### Compile

sumdat <- data.frame("Variable name" = nam, 
                     "Description" = desc, 
                     "Unique values" = uniqval, 
                     "Mean" = meanval, 
                     "Minimum" = rangemin, 
                     "Maximum" = rangemax)

sumdat[sumdat == 0 | sumdat == FALSE] = NA

write.csv(sumdat,
          file = "Meta-data_MD_2017-04.csv", 
          row.names=F)
