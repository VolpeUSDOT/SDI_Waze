# Aggregation of Waze and Bellevue crash data by segment and hour
# After completion, append weathers, FARS, and (when ready) LEHD to segments

# Notes: input shows suprisingly few Waze events on road segments. For instance, only 9 total Waze events on road segments on 2018-01-01. Load j = "2018-01", see table(format(Waze.seg.time$SegDayHour, '%d')). Can be true, Waze_snapped only 70,226 total for 2018. End result of segment aggregation and binding all months together ~ 60,000 rows. Checked against Waze_Snapped50ft_MatchName, these numbers are correct. Should also check against exported data from SDC.
# Bellevue crash events: when aggregated to segment and hour, have 1362 segment/hours with 1 crash, 3 segment/hours with 2 crashes. This seems reasonable: total was 2800 crash records, but majority are on interstates or SR 520.

#install.packages("circular")
rm(list=ls())
library(sp)
library(rgdal) # for readOGR(),writeOGR () needed for reading/writing in ArcM shapefiles
library(tidyverse)
library(doParallel)
library(circular)

codeloc <- ifelse(grepl('Flynn', normalizePath('~/')),
                  "~/git/SDI_Waze", "~/GitHub/SDI_Waze")

#source(file.path(codeloc, 'utility/get_packages.R')) #run if packages are needed

## Working on shared drive
wazeshareddir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2"
data.loc <- file.path(wazeshareddir, "Data/Bellevue")
output.loc <- file.path(data.loc, "Segments")

setwd(data.loc)

# Set up months to do ----

segfiles <- dir(output.loc)[grep('^WazeSegTimeList_', dir(output.loc))]

avail.months = substr(unlist(
  lapply(strsplit(segfiles, "_"), function(x) x[2])), 1, 7)

# Look for already completed months and skip those
done_files <- dir(output.loc)[grep("WazeSegTimeAll_", dir(output.loc))]
done.months <- substr(unlist(lapply(strsplit(done_files, "_"), function(x) x[2])), 1, 7)
todo.months = sort(avail.months)[avail.months %in% done.months]

#Use this line if you do not want to generate new files
#todo.months = sort(avail.months)[!avail.months %in% done.months] 

# Start aggregation by month ----
# The files have already been created, need to update this with a if exist clause.

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)

# writeLines(c(""), paste0("SegAgg_log.txt"))    

foreach(j = todo.months, .packages = c("dplyr", "lubridate", "utils", "circular")) %dopar% {
  #j = "2018-01"  
  # sink(paste0("SegAgg_log.txt"), append=TRUE)
  
  cat(paste(Sys.time()), j, "\n")                                                  
  load(file.path(output.loc, paste0("WazeSegTimeList_", j, ".RData"))) # includes both Waze (link.seg.waze) and crash data (link.seg.crash) 

  # Aggregate: new data frame will have one row per segment, per hour, per day.
  # Response variable column: count of unique crash events in this segment, within this time window. 
  # Counts for the number of Waze events of each alert_type and sub_type, inside this segment at this time.
  # TODO: include neighboring segments?
  
  StartTime <- Sys.time()
  Waze.seg.time$MagVar.circ <- circular(Waze.seg.time$magvar, units = "degrees", template = "geographics")
  Waze.seg.time$Sin.MagVar <- sin(Waze.seg.time$magvar)
  Waze.seg.time$Cos.MagVar <- cos(Waze.seg.time$magvar)
  waze.seg <- Waze.seg.time %>%
    group_by(RDSEG_ID,
             year = format(time, "%Y"), day = format(time, "%j"), hour = format(time, "%H"), weekday = format(time, "%u")) %>% # format(time, "%j") get the day of the year, not the day of the month.
    summarize(
      uniqueWazeEvents= n_distinct(SDC_uuid), # number of unique Waze events, includes 197 road closures.
      
      nWazeAccident = n_distinct(SDC_uuid[alert_type=="ACCIDENT"]),
      nWazeJam = n_distinct(SDC_uuid[alert_type=="JAM"]),
      nWazeRoadClosed = n_distinct(SDC_uuid[alert_type=="ROAD_CLOSED"]),
      nWazeWeatherOrHazard = n_distinct(SDC_uuid[alert_type=="WEATHERHAZARD"]),
      
      nHazardOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD"|sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"
                                           |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"
                                           |sub_type=="HAZARD_ON_ROAD_ICE"|sub_type=="HAZARD_ON_ROAD_LANE_CLOSED"
                                           |sub_type=="HAZARD_ON_ROAD_OBJECT"|sub_type=="HAZARD_ON_ROAD_POT_HOLE"
                                           |sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nHazardOnShoulder = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_SHOULDER"|sub_type=="HAZARD_ON_SHOULDER_ANIMALS"
                                               |sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"|sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"
                                               |sub_type=="HAZARD_ON_SHOULDER_MISSING_SIGN"]),
      nHazardWeather = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER"|sub_type=="HAZARD_WEATHER_FLOOD"
                                            |sub_type=="HAZARD_WEATHER_FOG"|sub_type=="HAZARD_WEATHER_HAIL"
                                            |sub_type=="HAZARD_WEATHER_MONSOON"|sub_type=="HAZARD_WEATHER_FREEZING_RAIN"
                                            |sub_type=="HAZARD_WEATHER_HEAVY_SNOW"|sub_type=="HAZARD_WEATHER_HEAVY_RAIN"]),
      
      nWazeAccidentMajor = n_distinct(SDC_uuid[sub_type=="ACCIDENT_MAJOR"]),
      nWazeAccidentMinor = n_distinct(SDC_uuid[sub_type=="ACCIDENT_MINOR"]),
      
      nWazeHazardCarStoppedRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_CAR_STOPPED"]),
      nWazeHazardCarStoppedShoulder = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_SHOULDER_CAR_STOPPED"]),
      nWazeHazardConstruction = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_CONSTRUCTION"]),
      nWazeHazardObjectOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_OBJECT"]),
      nWazeHazardPotholeOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_POT_HOLE"]),
      nWazeHazardRoadKillOnRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_ROAD_KILL"]),
      nWazeJamModerate = n_distinct(SDC_uuid[sub_type=="JAM_MODERATE_TRAFFIC"]),
      nWazeJamHeavy = n_distinct(SDC_uuid[sub_type=="JAM_HEAVY_TRAFFIC"]),
      nWazeJamStandStill = n_distinct(SDC_uuid[sub_type=="JAM_STAND_STILL_TRAFFIC"]),
      nWazeWeatherFlood = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER_FLOOD"]),
      nWazeWeatherFog = n_distinct(SDC_uuid[sub_type=="HAZARD_WEATHER_FOG"]),
      nWazeHazardIceRoad = n_distinct(SDC_uuid[sub_type=="HAZARD_ON_ROAD_ICE"]),
      
      #Omit road closures from counts (not reported by users)
      nWazeRT3 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="3"]),
      nWazeRT4 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="4"]),
      nWazeRT6 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="6"]),
      nWazeRT7 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="7"]),
      nWazeRT2 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="2"]),
      nWazeRT0 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="0"]),
      nWazeRT1 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="1"]),
      nWazeRT20 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="20"]),
      nWazeRT17 = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & roadclass=="17"]),
      
      #Values corrected to represent N, NE, SE, S, EW, NW directions.
      #Omit road closures - magvar is filled in as zero for road closures, but it does not reflect direction
      nMagVar330to30N = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 330 | magvar<30]),
      nMagVar30to90NE = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 30 & magvar<90]),
      nMagVar90to150SE = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 90 & magvar<150]),
      nMagVar150to210S = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 150 & magvar<210]),
      nMagVar210to270SW = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 210 & magvar<270]),
      nMagVar270to330NW = n_distinct(SDC_uuid[alert_type!="ROAD_CLOSED" & magvar>= 270 & magvar<330]),
      
      #This will return warnings for the grid cells/times without any magvar data (roads closed only?)
      meanCirMagVar = mean.circular(MagVar.circ[alert_type!="ROAD_CLOSED"]),
      medCirMagVar = median.circular(MagVar.circ[alert_type!="ROAD_CLOSED"])
      ) 

  #Compute grid counts for Bellevue crash data
  # Add accident severity counts by grid cell 
  # names(Crash.seg.time)
  crash.seg <- 
    Crash.seg.time %>%
    group_by(RDSEG_ID, year = format(time, "%Y"), day = format(time, "%j"), hour = format(time, "%H")) %>%
    summarize(
      uniqueCrashreports= n_distinct(REPORT_NUM),
      nCrashInjuryFatal = n_distinct(REPORT_NUM[FATAL_CRAS == 1]),
      nCrashSeriousInjury = n_distinct(REPORT_NUM[SERIOUS_IN == 1]),
      nCrashEvidentInjury = n_distinct(REPORT_NUM[EVIDENT_IN == 1]),
      nCrashPossibleInjury = n_distinct(REPORT_NUM[POSSIBLE_I == 1]),
      nCrashKSI = n_distinct(REPORT_NUM[SERIOUS_IN == 1 | FATAL_CRAS == 1]),
      nCrashAllInjury = n_distinct(REPORT_NUM[SERIOUS_IN == 1 | EVIDENT_IN == 1 | POSSIBLE_I == 1]),
      nCrashEvdPossInjury = n_distinct(REPORT_NUM[EVIDENT_IN == 1 | POSSIBLE_I == 1]),
      nCrashPDO = n_distinct(REPORT_NUM[PDO___NO_I == 1 ]),
      nCrashWorkzone = n_distinct(REPORT_NUM[!is.na(WORKZONE)]) # Number of crashes happened in a workzone
    ) 
  
  #Merge  crash counts to waze counts by segment ID 
  names(waze.seg)
  names(crash.seg)
  
  wazeTime.crash.seg <- full_join(waze.seg, crash.seg, by = c("RDSEG_ID", "year", "day", "hour")) 
  wazeTime.crash.seg[is.na(wazeTime.crash.seg)] <- 0
  # %>% mutate_all(list(replace(., is.na(.), 0))) # keep having error running this code, probably because the packages have been updated.
  # Replace NA with zero (for the grid counts here, 0 means there were no reported Waze events or Bellevue crashes in the segment at that hour)
  
  # Update time variable 
  segtimeChar <- paste(paste(wazeTime.crash.seg$year, wazeTime.crash.seg$day, sep = "-"), wazeTime.crash.seg$hour, sep=" ") # day is the day of the year
  wazeTime.crash.seg$segtime <- segtimeChar # strptime(segtimeChar, "%Y-%j %H", tz = 'America/Los_Angeles')
  
  class(wazeTime.crash.seg) <- "data.frame" # POSIX date/time not supported for grouped tbl
  
  fn = paste("WazeSegTimeAll_", j, ".RData", sep="")
  
  save(list="wazeTime.crash.seg", file = file.path(output.loc, fn))
  
  EndTime <- Sys.time()-StartTime
  cat(j, 'completed', round(EndTime, 2), attr(EndTime, 'units'), '\n')
  
} # End month aggregation loop ----


stopCluster(cl)

# Stack and add additional variables ----

# First, loop over all months and bind together
w.all <- vector()

for(j in avail.months){ # we need the done.months instead of todo.months
  fn = paste("WazeSegTimeAll_", j, ".RData", sep="")
  load(file.path(output.loc, fn))
  w.all <- rbind(w.all, wazeTime.crash.seg)
}

dim(w.all) # 61237*55, added more variables

#check magvar (circular variable)
#plot.circular(w.all$meanCirMagVar)
#hist(as.numeric(w.all$meanCirMagVar))
#length(which(w.all$meanCirMagVar==0)) #10% are zeros - check original data     
#for month 1, all roads closed have zero entered along with 11/122 accidents, 376/4220 jams, and 193/1488 weatherhazard
#many zeros are likely "real" (indicate due North)

# Convert time variable to time format, prepare for temporal analysis
w.all$time_hr <- as.POSIXct(w.all$segtime, "%Y-%j %H", tz = 'America/Los_Angeles')

# Weather ----
# Load weather data -- Only need to assign to a Date
load(file.path(data.loc, "Weather","Prepared_Bellevue_Wx_2018.RData")) # the file name wx.grd.day
# Fill NA with 0s
wx.grd.day[is.na(wx.grd.day)] = 0 

# Format date and time to match the variables in w.all
wx.grd.day = wx.grd.day %>%
  mutate(year = format(day, '%Y'),
         day = format(day, '%j'))

# Left join to w.all
w.all <- left_join(w.all, wx.grd.day, by = c('RDSEG_ID'='ID', 'year', 'day'))

# FARS ----
FARS_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "FARS_Snapped50ft_MatchName") 
# 13 x 19 

# Join only segment; tabulate to get counts across segments 
FARS_segment = FARS_snapped@data %>%
  group_by(RDSEG_ID) %>%
  summarize(nFARS = n())

w.all <- left_join(w.all, FARS_segment, by = 'RDSEG_ID')


# Fill 0s
#This affects the distribution of the numeric variables - mean/median of magvar. now that we removed the medMagVar, so this is fine.
w.all[is.na(w.all)] = 0

# Joined the BellevueSegment data (e.g., ) ----
# Load the network data, for the definitive RDSEG_ID to join to
roadnettb_snapped <- readOGR(dsn = file.path(data.loc, "Shapefiles"), layer = "RoadNetwork_Jurisdiction_withData") # 6647 * 14, new: 6647*38
names(roadnettb_snapped)

w.all <- left_join(w.all, roadnettb_snapped@data, by = 'RDSEG_ID')

# don't think we need to fill NAs with zeros.
names(w.all)
# Compare nFARS and nFARS_1217
w.all[, c("nFARS", "nFARS_1217")] 
sum(as.numeric(w.all$nFARS) - as.numeric(w.all$nFARS_1217)) # -61237, The two columns are not equal.
w.all[as.numeric(w.all$nFARS) != as.numeric(as.character(w.all$nFARS_1217)), c("nFARS", "nFARS_1217")] # Great. Two columns match. Need to convert a factor to character then to numeric.

# data check between uniqueCrashreports and nCrashes
w.all[as.numeric(w.all$uniqueCrashreports) != as.numeric(as.character(w.all$nCrashes)), c("uniqueCrashreports", "nCrashes")]
# nCrashes is the total # of crashes of a segment at all hours
# uniqueCrashreports is the # of crash of a segment at each hour.

# We found many variables are not in its correct data type, so Convert a few variables from factor to numeric, due to GIS auto-converting numeric columns to factors
sapply(roadnettb_snapped@data,class) # all but "Shape_STLe" are factor type
numeric_var <- c("SpeedLimit", "nWaze_All", "nWazeAcc", "nCrashes", "Crash_End1", "Crash_End2", "nBikes", "nFARS_1217")
w.all[, numeric_var] <- lapply(w.all[, numeric_var], function(x) as.numeric(as.character(x)))

# Create binary crash indicator:
w.all$biCrash <- ifelse(w.all$uniqueCrashreports > 0, 1, 0)

#Create crash response variables scaled by segment length
w.all$CrashPerMile = as.numeric(w.all$uniqueCrashreports/(w.all$Shape_STLe*0.000189394))

# Save joined output ---

fn = paste("Bellevue_Waze_Segments_", 
           avail.months[1], '_to_', avail.months[length(avail.months)], 
           ".RData", sep="")

save(list="w.all", file = file.path(output.loc, fn))

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Variables organization: Aggregate data to 4 hour window ----
# Reset output location to save files to model output

seg.loc <- file.path(data.loc, "Segments")
output.loc <- file.path(data.loc, "Model_output")
visual.loc <- file.path(data.loc, "Model_visualizations")

# Check if prepared data are available; if not, run Segment Aggregation.
Waze_Prepared_Data = dir(seg.loc)[grep("^Bellevue_Waze_Segments_", dir(seg.loc))][1] #"Bellevue_Waze_Segments_2018-01_to_2018-12.RData"

if(length(grep(Waze_Prepared_Data, dir(seg.loc))) == 0){
  stop(paste("No Bellevue segment data available in", seg.loc, "\n Run Segment_Aggregation_Bell.R or check network connection"))
}  else {
  load(file.path(seg.loc, Waze_Prepared_Data))
}

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Bellevue travel demand model used 6-9 and 3-6 pm, our aggregation should include these two periods, so we can do a crash risk model at these two peak periods of a day.
# Two ways to aggregate the hour window
w.all$grp_varhour <- ifelse(w.all$hour %in% c("00", "01", "02","03","04","05"), "Early AM", 
                            ifelse(w.all$hour %in% c("06", "07", "08", "09"), "AM Peak", 
                                   ifelse(w.all$hour %in% c("10", "11", "12", "13", "14"), "Mid-day",
                                          ifelse(w.all$hour %in% c("15", "16", "17", "18"), "PM Peak",
                                                 ifelse(w.all$hour %in% c("19", "20", "21", "22", "23"), "Evening", NA)))))

# four hour window
w.all$grp_name <- ifelse(w.all$hour %in% c("03","04","05", "06"), "Early AM", 
                         ifelse(w.all$hour %in% c("07", "08", "09", "10"), "AM Peak", 
                                ifelse(w.all$hour %in% c( "11", "12","13", "14"), "Mid-day",
                                       ifelse(w.all$hour %in% c("15", "16", "17", "18"), "PM Peak",
                                              ifelse(w.all$hour %in% c("19", "20", "21", "22"), "Evening",
                                                     ifelse(w.all$hour %in% c("23", "00", "01", "02"), "Mid-night", NA))))))

w.all <- w.all %>% mutate(time_hr = as.POSIXct(segtime, '%Y-%j %H', tz = 'America/Los_Angeles'),
                          date = as.Date(time_hr, format = '%Y-%j %H', tz = 'America/Los_Angeles'), # need to set them the same timezone, otherwise, some 2018-12-31 records become 2019-01-01 records.
                          month = as.Date(cut(date, breaks = "month"), tz = 'America/Los_Angeles'),
                          wkday = as.factor(weekdays(date))
)

stopifnot(with(w.all, date >= '2018-01-01' & date <= '2018-12-31')) # make sure that all dates are within calendar year 2018.

# all crash and Waze variables need to be aggregated by hour and segment
# load the aggregation function
source(file.path(codeloc, 'WA/utility/aggregation_fun().R'))

t_var = "day"
w.all.4hr <- agg_fun(w.all, t_var)
# Add crash counts per mile
w.all.4hr$CrashPerMile = as.numeric(w.all.4hr$uniqueCrashreports/(w.all.4hr$Shape_STLe*0.000189394))
hist(log(w.all.4hr$CrashPerMile)) #relatively normal distribution
names(w.all.4hr)
dim(w.all.4hr)

t_var = "wkday"
w.all.4hr.wd <- agg_fun(w.all, t_var)
w.all.4hr.wd$CrashPerMile = as.numeric(w.all.4hr.wd$uniqueCrashreports/(w.all.4hr.wd$Shape_STLe*0.000189394))
hist(log(w.all.4hr.wd$CrashPerMile)) #relatively normal distribution
names(w.all.4hr.wd)
dim(w.all.4hr.wd)

t_var = c("month")
w.all.4hr.mo <- agg_fun(w.all, t_var)
w.all.4hr.mo$CrashPerMile = as.numeric(w.all.4hr.mo$uniqueCrashreports/(w.all.4hr.mo$Shape_STLe*0.000189394))
hist(log(w.all.4hr.mo$CrashPerMile)) #relatively normal distribution
names(w.all.4hr.mo)
dim(w.all.4hr.mo)

t_var = c("month", "wkday")
w.all.4hr.mo.wd <- agg_fun(w.all, t_var)
w.all.4hr.mo.wd$CrashPerMile = as.numeric(w.all.4hr.mo.wd$uniqueCrashreports/(w.all.4hr.mo.wd$Shape_STLe*0.000189394))
hist(log(w.all.4hr.mo.wd$CrashPerMile)) #relatively normal distribution
names(w.all.4hr.mo.wd)
dim(w.all.4hr.mo.wd)

t_var = c("year")
w.all.4hr.yr <- agg_fun(w.all, t_var)
w.all.4hr.yr$CrashPerMile = as.numeric(w.all.4hr.yr$uniqueCrashreports/(w.all.4hr.yr$Shape_STLe*0.000189394))
hist(log(w.all.4hr.yr$CrashPerMile)) #relatively normal distribution
names(w.all.4hr.yr)
dim(w.all.4hr.yr)

w.all.yr.seg <- agg_fun_seg(w.all)
w.all.yr.seg$CrashPerMile = as.numeric(w.all.yr.seg$uniqueCrashreports/(w.all.yr.seg$Shape_STLe*0.000189394))
hist(log(w.all.yr.seg$CrashPerMile)) #relatively normal distribution
names(w.all.yr.seg)
dim(w.all.yr.seg)

# examine the sparsity of the crash reports, does not improve a lot
table(w.all.4hr$uniqueCrashreports)
# 0     1     2 
# 50239  1358     6 
table(w.all.4hr.wd$uniqueCrashreports)
# 0     1     2     3     4 
# 11609  1146    82    16     3 
table(w.all.4hr.mo$uniqueCrashreports)
# 0     1     2     3 
# 16300  1248    52     6 
table(w.all.4hr.mo.wd$uniqueCrashreports)
# 0     1     2     3 
# 34382  1341    13     1 
table(w.all.4hr.yr$uniqueCrashreports)
#   0    1    2    3    4    5    6    7    8    9   10 
#3407  697  163   54   23    7    4    1    1    1    1
table(w.all.yr.seg$uniqueCrashreports)
#0    1    2    3    4    5    6    7    8    9   10   11   13   16   18   20 
#1096  304  117   56   32   15   22   15    9    6    2    1    1    1    1    1 

# examine for arterials only
with(w.all.4hr %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))
with(w.all.4hr.wd %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))
with(w.all.4hr.mo %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))
with(w.all.4hr.mo.wd %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))
with(w.all.4hr.yr %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))
with(w.all.yr.seg %>% filter(!ArterialCl %in% "Local"), table(uniqueCrashreports))

# Are these high counts segments the same or clustered? (need to see in the map)
unique(w.all.4hr.mo.wd$RDSEG_ID[w.all.4hr.mo.wd$uniqueCrashreports %in% c(2,3)])
# [1] "1"    "1368" "1737" "2034" "2062" "3079" "325"  "4494" "4515" "4516" "5120" "5122" "9290" "9538" "9865"
# in ArcGIS, use this SQL code to select these segments: "RDSEG_ID" IN (1,   1368, 1737, 2034, 2062, 3079, 325,  4494, 4515, 4516, 5120, 5122, 9290, 9538,
#                9865)

# Save the 4 hour data as Rdata
fn = "Bellevue_Waze_Segments_2018-01_to_2018-12_4hr.RData"

save(list= c("w.all", "w.all.4hr", "w.all.4hr.wd", "w.all.4hr.mo", "w.all.4hr.mo.wd", "w.all.4hr.yr", "w.all.yr.seg"), file = file.path(seg.loc, fn))


