
## Set up

source2 <- function(file, start, end, ...) {
  file.lines <- scan(file, what=character(), skip=start-1, nlines=end-start+1, sep='\n')
  file.lines.collapsed <- paste(file.lines, collapse='\n')
  source(textConnection(file.lines.collapsed), ...)
}

source2("SpecialEventsAnalysis.R", 3, 79) # "Set up" section in SpecielEventsAnalysis.R
source2("SpecialEventsAnalysis.R", 233, 272) # partial "Data for Time Series" section in SpecielEventsAnalysis.R


#######################################################
# Visualization Only
#######################################################
#### Special Events Time Series Plots ####
loc = "SE2"
buf = 3
dt <- GridDataSE(loc,buf,alldays,col.names)

# By Event Type
dt_EventType <- dt %>% group_by(EventType, weekday, hour) %>% summarize(nWazeJam = sum(nWazeJam),
                                                         Obs = sum(Obs),
                                                         nWazeAccident = sum(nWazeAccident),
                                                         nHazardOnShoulder = sum(nHazardOnShoulder),
                                                         nHazardOnRoad = sum(nHazardOnRoad),
                                                         nHazardWeather = sum(nHazardWeather)
                                                         )

#Prep for regression
names(dt_EventType)
hist(dt_EventType$nWazeAccident)

#data are counts - log transform (counts plus constant)
dt_EventType <- dt_EventType %>% mutate(lognWazeAccident = log(nWazeAccident+1), logObs = log(Obs+1))
dt_EventType <- droplevels(dt_EventType)
table(dt_EventType$EventType)

hist(dt_EventType$lognWazeAccident)
hist(dt_EventType$logObs)

#ANOVA - regression model, but looking at features rather than prediction properties
lm_fitWazeAcc <- lm(lognWazeAccident ~ EventType + weekday, data = dt_EventType)
summary(lm_fitWazeAcc)

lm_fitEDTObsAcc <- lm(logObs ~ EventType + weekday, data = dt_EventType)
summary(lm_fitEDTObsAcc)

#chi square
chiTableEDT <- table(dt_EventType$Obs, dt_EventType$EventType)
chisqEDT <- chisq.test(chiTableEDT)
chisqEDT

chiTableWaze <- table(dt_EventType$nWazeAccident, dt_EventType$EventType)
chisqWaze <- chisq.test(chiTableWaze)
chisqWaze

chiWaze <- dt_EventType %>% group_by(EventType) %>% summarize(nWazeAccTotal = sum(nWazeAccident))
chiWaze <- chiWaze%>%
      as.data.frame()



ggplot(dt_EventType, aes(x = hour, y = nWazeJam)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average Waze Jam") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt_EventType, aes(x = hour, y = Obs)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average EDT Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt_EventType, aes(x = hour, y = nWazeAccident)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average Waze Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt_EventType, aes(x = hour, y = nHazardOnShoulder)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average Waze Hazard On Shoulder") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt_EventType, aes(x = hour, y = nHazardOnRoad)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average Waze Hazard On Road") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt_EventType, aes(x = hour, y = nHazardWeather)) + geom_point() + geom_line() + facet_wrap(~ EventType) + ylab("Average Waze Hazard or Weather") + ggtitle(paste(loc, buf,"mile buffer"))

# Scatter plot
ggplot(dt, aes(x = factor(hour), y = nWazeJam)) + geom_point(alpha = 0.2) + facet_wrap(~ EventType) + ylab("Number of Waze Jam") + ggtitle(paste(loc, buf,"mile buffer"))

# boxplot
ggplot(dt, aes(x = factor(hour), y = nWazeJam)) + geom_boxplot() + facet_wrap(~ EventType) + ylab("Number of Waze Jam") + ggtitle(paste(loc, buf,"mile buffer"))

# By date, averages are over 42 grids
dt_Date <- dt %>% group_by(date, DayofWeek, hour) %>% summarize(nWazeJam = mean(nWazeJam),
                                                               Obs = mean(Obs),
                                                               nWazeAccident = mean(nWazeAccident),
                                                               nHazardOnShoulder = mean(nHazardOnShoulder),
                                                               nHazardOnRoad = mean(nHazardOnRoad),
                                                               nHazardWeather = mean(nHazardWeather)
) %>% mutate(Date = paste(date, DayofWeek), Month = month(date))

dt_Date <- dt %>% group_by(date, DayofWeek, hour) %>% summarize(nWazeJam = sum(nWazeJam),
                                                                Obs = sum(Obs),
                                                                nWazeAccident = sum(nWazeAccident),
                                                                nHazardOnShoulder = sum(nHazardOnShoulder),
                                                                nHazardOnRoad = sum(nHazardOnRoad),
                                                                nHazardWeather = sum(nHazardWeather)
) %>% mutate(Date = paste(date, DayofWeek))


weekday = "Sunday"
ggplot(dt_Date %>% filter(DayofWeek == weekday), aes(x = hour, y = nWazeJam, group = date)) +
  # geom_point(alpha = 0.5, color = "red") +
  geom_line(alpha = 0.2, color = "blue") +
  facet_wrap(~ Date) +
  ylab("Average Waze Jam") + ggtitle(paste(loc, buf,"mile buffer", weekday))
# consider an area/density chart

weekday = "Saturday"
ggplot(dt_Date %>% filter(DayofWeek == weekday), aes(x = hour, y = nWazeJam, group = date)) +
  # geom_point(alpha = 0.5, color = "red") +
  geom_line(alpha = 0.2, color = "blue") +
  facet_wrap(~ Date) +
  ylab("Average Waze Jam") + ggtitle(paste(loc, buf,"mile buffer", weekday))
# consider an area/density chart

# Select Sunday, averages are over 42 grid cells
weekday = "Sun"

# Create a new varaible EventDay for a specific day of week.
dt_Sun <- dt %>% filter(DayofWeek == "Sun") %>% mutate(EventDay = ifelse(EventType != "NoEvent", paste0(date, EventType), "NoEvent")) %>% group_by(EventDay, hour) %>% summarize(nWazeJam = mean(nWazeJam),
                               Obs = mean(Obs),
                               nWazeAccident = mean(nWazeAccident),
                               nHazardOnShoulder = mean(nHazardOnShoulder),
                               nHazardOnRoad = mean(nHazardOnRoad),
                               nHazardWeather = mean(nHazardWeather),
)

ggplot(dt_Sun, aes(x = hour, y = nWazeJam)) + geom_point() + geom_line() + facet_wrap(~ EventDay) + ylab("Average Waze Jam") + ggtitle(paste(loc, buf,"mile buffer","Sunday"))

# Select Tuesday, averages are over 42 grid cells
weekday = "Tue"
dt_Tue <- dt %>% filter(DayofWeek == weekday) %>% mutate(EventDay = ifelse(EventType != "NoEvent", paste0(date, EventType), "NoEvent")) %>% group_by(EventDay, hour) %>% summarize(nWazeJam = mean(nWazeJam),
                                                                                                                                                                           Obs = mean(Obs),
                                                                                                                                                                           nWazeAccident = mean(nWazeAccident),
                                                                                                                                                                           nHazardOnShoulder = mean(nHazardOnShoulder),
                                                                                                                                                                           nHazardOnRoad = mean(nHazardOnRoad),
                                                                                                                                                                           nHazardWeather = mean(nHazardWeather)
)

# Scatter Plot
ggplot(dt_Tue, aes(x = hour, y = nWazeJam)) + geom_point() + geom_line() + facet_wrap(~ EventDay) + ylab("Average Waze Jam") + ggtitle("3 mile buffer, Tuesdays")

ggplot(dt, aes(x = factor(hour), y = Obs)) + geom_point(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number EDT Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nWazeAccident)) + geom_point(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nWazeJam)) + geom_point(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Jam") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nHazardOnShoulder)) + geom_point(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Hazard On Shoulder") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nHazardOnRoad)) + geom_point(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Hazard On Road") + ggtitle(paste(loc, buf,"mile buffer"))

# Boxplot
ggplot(dt, aes(x = factor(hour), y = Obs)) + geom_boxplot(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number EDT Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nWazeAccident)) + geom_boxplot(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Accident") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nWazeJam)) + geom_boxplot(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Jam") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nHazardOnShoulder)) + geom_boxplot(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Hazard On Shoulder") + ggtitle(paste(loc, buf,"mile buffer"))

ggplot(dt, aes(x = factor(hour), y = nHazardOnRoad)) + geom_boxplot(alpha = 0.2, color = "blue") + facet_wrap(~ EventType) + ylab("Number Waze Hazard On Road") + ggtitle(paste(loc, buf,"mile buffer"))

#### Special Events Mapping ####
plot(grid)
points(SpecialEventsExpand_SP, col = "red") # 0.25 sq mile buffer

#### Special events mini example - Work on the Week ending 8/17/2018, move to the end of the script ####
# Two example events, Baseball game. Fedex Field does not have football event on Sep 17, and M&T Bank Stadium does not have football event on Sept 10. We need to check whether there is any pre-season football events before Sept 10 to have a baseline to compare.
"1:00 PM ETSeptember 10, 2017, FedEx Field, 1600 Fedex Way, Landover, MD 20785
1:00 PM ETSeptember 17, 2017, M&T Bank Stadium, 1101 Russell St, Baltimore, MD 21230"
"(38.907794, -76.864535) (39.278187, -76.622329)"
"(FC-64, C-63, FB-64) (FK-38, FK-37, FJ-38)"

# Compare hourly distribution of these pologans, they are mostly zero.
GridCount[GridCount$GRID_ID %in% paste0(1,c("FC-64", "C-63", "FB-64")) & GridCount$date == "2017-09-10",]
# they are all zero, not crashes happened at this day.

AllModel30[AllModel30$GRID_ID %in% paste0(1,c("FC-64", "C-63", "FB-64"))
                    & AllModel30$date == "2017-09-10", c("GRID_ID","hour","nWazeJam","nWazeHazardCarStoppedRoad","nWazeHazardCarStoppedShoulder","nHazardOnRoad")]
# they are all zero, not jams happened at this day.

# Write special events into a table
SpecialEvents <- data.frame(location = c("Fedex Field", "Fedex Field"),
                            event = c("Football@1pm", "No Event"),
                            date = c("2017-09-10", "2017-09-17"),
                            day = c(253, 260),
                            lon = c(-76.864535, -76.864535),
                            lat = c(38.907794,38.907794),
                            buffer = 3) # FedEx Field
write.csv(SpecialEvents, paste0(localdir, "/Special Events/SpecielEvents.csv"), row.names = F)

SpecialEvents_SP <- SpatialPointsDataFrame(SpecialEvents[c("Lon", "Lat")], SpecialEvents, proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure Waze data is a SPDF
SpecialEvents_SP <-spTransform(SpecialEvents_SP, CRS(proj.USGS)) # create spatial point data frame
plot(SpecialEvents_SP)

buffdist <- SpecialEvents$buffer*1609 # convert miles to meters
SpecialEvents_buffer <- gBuffer(SpecialEvents_SP, width = buffdist[1]) # create a buffer, look for buffer_state.R for more information on spatial join

plot(SpecialEvents_buffer) # plot the spatial

grid@data # look at data from the grid
# over(SpecialEvents_buffer, grid, fn = mean) # join polygons to SpatialPolygonDataFrame, return to only one row of mean values in the data frame.
gIntersects(SpecialEvents_buffer, grid, byid = T) # a data frame with T/F logic values of the grid data frame
sum(gIntersects(SpecialEvents_buffer, grid, byid = T)) # 42 polygons are intersected

grid_id <- grid$GRID_ID[gIntersects(SpecialEvents_buffer, grid, byid = T)]
grid_id <- paste0(1,grid_id)

grid_overlap <- expand.grid(GRID_ID = grid_id, hour = c(0:23), day = c(253, 260))

# SpecialEvents <- SpecialEvents %>% left_join(grid@data)

# GridCount[GridCount$GRID_ID %in% grid_id & GridCount$date == "2017-09-10",]

# # fill in the GridCount for all hours
# blank.grid <- expand(GridCount, GRID_ID, day, hour)
# blank.grid

# Columns with numeric counts or values
col.names <- names(GridCount)[-c(1:4,15)]

# GridCount_new <- blank.grid %>% left_join(GridCount, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31")) # 30256488 = 24*183*6889

# dim(GridCount_new) #30256488       15
length(unique(GridCount$day)) #183
length(unique(GridCount$date)) #183
length(unique(GridCount$GRID_ID)) #6889

# t-test accident counts between event and a week after the event
y <- grid_overlap %>% left_join(GridCount, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31"), DayOfWeek = as.integer(factor(weekdays(date),levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))) %>% left_join(SpecialEvents, by =  c("day") )

y1 <- y %>% filter(day == 253)

y2 <- y %>% filter(day == 260)

# T-test on accidents between two days.
t.test(y1$nWazeAccident,y2$nWazeAccident,paired=TRUE) # if buffer = 2 mile, p-value = 0.7154, not significant; buffer = 3 mile, p-value=0.035, significant

# remove all other objects, and run the script again
# rm(list=setdiff(ls(), "AllModel30"))

# Time series plot
y_sum <- y %>% group_by(event, hour) %>% summarize(nWazeAccident = mean(nWazeAccident))
ggplot(y_sum, aes( x = hour, y = nWazeAccident)) + geom_point() + geom_line() + facet_wrap(~ event) + ylab("Average Waze Accident")
ggsave(paste0(wazedir,"/Output/visualizations/Special_event_example1.png"))

# Other variables
col.names <- c("nWazeJam")

# t-test accident counts between event and a week after the event
y <- grid_overlap %>% left_join(AllModel30, by = c("GRID_ID", "day", "hour")) %>% mutate_if(colnames(.) %in% col.names,funs(replace(., which(is.na(.)), 0))) %>% mutate(date = as.Date(day, origin = "2016-12-31"), DayOfWeek = as.integer(factor(weekdays(date),levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))) %>% left_join(SpecialEvents, by =  c("day") )

y1 <- y %>% filter(day == 253)

y2 <- y %>% filter(day == 260)

# T-test on accidents between two days.
t.test(y1$nWazeJam,y2$nWazeJam,paired=TRUE) #  buffer = 3 mile, p-value=0.2326

# remove all other objects, and run the script again
# rm(list=setdiff(ls(), "AllModel30"))

# Time series plot
y_sum <- y %>% group_by(event, hour) %>% summarize(nWazeJam = mean(nWazeJam))
ggplot(y_sum, aes( x = hour, y = nWazeJam)) + geom_point() + geom_line() + facet_wrap(~ event) + ylab("Average Waze Jam")
ggsave(paste0(wazedir,"/Output/visualizations/Special_event_example1_Jam.png"))


