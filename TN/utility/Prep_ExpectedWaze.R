# Generate expected Waze events by grid cell by: month of year, day of week, and hour of day
# We will use the last 12 months from today of Waze data

# Start from PredictWeek_TN.R, where w.allmonths is already loaded

# Test code for 1% of the data
# w.test <- w.allmonths[sample(1:nrow(w.allmonths), nrow(w.allmonths)/100, replace = F),]
# 
# w.expected <- w.test %>%
#   group_by(GRID_ID, mo, DayOfWeek, hour) %>%
#   summarize(nWazeAccident = median(nWazeAccident, na.rm=T),
#             nWazeWeatherOrHazard = median(nWazeWeatherOrHazard, na.rm=T),
#             nWazeJam = median(nWazeJam, na.rm=T),
#             nWazeRoadClosed = median(nWazeRoadClosed, na.rm=T)
#             )


waze_date_range <- paste(range(w.allmonths$date), collapse = "_to_")

prepname =  paste0("TN_Waze_Expected_", g, "_", waze_date_range, ".RData")

if(!file.exists(file.path(localdir, 'Waze', prepname))){

w.expected <- w.allmonths %>%
  group_by(GRID_ID, mo, DayOfWeek, hour) %>%
  summarize(nWazeAccident = median(nWazeAccident, na.rm = T),
            nWazeWeatherOrHazard = median(nWazeWeatherOrHazard, na.rm = T),
            nWazeJam = median(nWazeJam, na.rm = T),
            nWazeRoadClosed = median(nWazeRoadClosed, na.rm = T)
            )

save(list = c("w.expected"), 
     file = file.path(localdir, "Waze", prepname))

# Copy to S3
system(paste("aws s3 cp",
             file.path(localdir, "Waze", prepname),
             file.path(teambucket, state, "Waze", prepname)))

} else {
  load(file.path(localdir, "Waze", prepname))
}

