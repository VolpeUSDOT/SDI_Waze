# Info --------------------------------------------------------------------
# Purpose: Merges hourly weather data with road segments. 
# Author: Joey Reed (joseph.reed1@dot.gov)
# Last edited: 3/25/2024

# Notes: 
# Some crazy high numbers for precip and temp. 
# There is still a lot of NAs for snow when doing it weekly
# Need more weather data, and need to filter for one year in the current timezone 

# Prep --------------------------------------------------------------------

# directory 
inputdir <- file.path(getwd(), "Input")

# Load packages 
library(tidyverse)
library(sf)
library(data.table) # contains FREAD

# Projection 
projection <- 5070 

year <- 2020 # testubg with 2021


# Functions --------------------------------------------------------------

#US_timezones <- st_read(file.path(inputdir, "Shapefiles", "TimeZone", "combined-shapefile.shp"))
# this shapefile above doesn't have adjustments in it, just names. I suggest we download the one below of the SDC or get access to the website.
US_timezones <- st_read("https://geo.dot.gov/server/rest/services/Hosted/Time_Zones_DS/FeatureServer/0/query?returnGeometry=true&where=1=1&outFields=*&f=geojson")
timezone_adj <- US_timezones %>% st_transform(crs=projection) %>% 
  mutate(adjustment = as.numeric(paste0(str_sub(utc, 1, 1), str_sub(utc, 2, 3)))) %>% 
  select(adjustment)

# Hourly Data Prep --------------------------------------------------------

# Read in list of GHCN hourly stations
stations <- read.csv(file=file.path(inputdir, "Weather","GHCN", "Hourly","ghcnh-station-list.csv"), 
                     header = FALSE, strip.white = TRUE)
stations <- stations[,1:6]
colnames(stations) <- c('Station_ID','Latitude','Longitude','Elevation','State','Station_name') 

# Filter down to the list of stations in the state of interest
state_stations <- stations[stations$State==state,]


load_statehourly <- function(year_var){
# Filter down to the list of stations for which we have hourly data in the year of interest
available <- logical(nrow(state_stations))
for (i in 1:nrow(state_stations)) {
  ID = state_stations[i,'Station_ID']
  filename = paste0('GHCNh_',ID,'_',year_var,'.psv')
  print(filename) # check what it is checking
  ifelse(file.exists(file.path(inputdir, "Weather", "GHCN", "Hourly", year_var, state, filename)
  ),
  available[i] <- TRUE,
  available[i] <- FALSE
  )
}
hr_state_stations <- state_stations[available,]

# Initialize empty dataframe to store the hourly data from the files
state_hourly_hist_weather <- data.frame(Station_ID=character(),
                                        Station_name=character(),
                                        Year=integer(),
                                        Month=integer(),
                                        Day=integer(),
                                        Hour=integer(),
                                        Latitude=numeric(),
                                        Longitude=numeric(),
                                        Elevation=numeric(),
                                        temperature=numeric(),
                                        precipitation=numeric(),
                                        snow_depth=numeric()
)

# Read in the data from each station file for the state and compile
for (i in 1:nrow(hr_state_stations)){
  ID = hr_state_stations[i,'Station_ID']
  filename = paste0('GHCNh_',ID,'_',year_var,'.psv')
  df_i = read.table(file.path(inputdir, "Weather", "GHCN", "Hourly", year_var, state, filename),
                    header=TRUE,
                    sep = "|",
                    fill = TRUE
  ) %>% dplyr::select('Station_ID',
                      'Station_name', 
                      'Year', 
                      'Month', 
                      'Day', 
                      'Hour', 
                      'Latitude', 
                      'Longitude', 
                      'Elevation', 
                      'temperature', 
                      'precipitation', 
                      'snow_depth')
  state_hourly_hist_weather <- rbind(state_hourly_hist_weather,df_i)
}
return(state_hourly_hist_weather)
}

state_hourly_hist_weather1 <- load_statehourly(year_var = year)

# calculate stations timezone adjustments 
tz_adjustments <- stations %>% filter(State == state) %>% 
  distinct(Station_ID, .keep_all = T) %>% 
  st_as_sf(coords = c("Longitude", "Latitude"), crs=4326) %>% # these are in crs 4326, so need to load, then convert 
  st_transform(crs = projection) %>% 
  st_join(timezone_adj, join=st_intersects) %>% 
  st_drop_geometry() %>%
  select(Station_ID, adjustment)

getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

if(getmode(tz_adjustments$adjustment)<0){ # if the timezone is < 0 
  year2 = year+1
}

if(getmode(tz_adjustments$adjustment)>0){
  year2 = year-1
}

if(getmode(tz_adjustments$adjustment)!=0){ # here in case utc = 0 which is currently impossible
  state_hourly_hist_weather2 <- load_statehourly(year_var = year2)
}

wx <- state_hourly_hist_weather1 %>% bind_rows(state_hourly_hist_weather2) %>% 
  left_join(tz_adjustments, by="Station_ID") %>% 
  mutate(Date = ymd_h(paste0(Year, "-", Month, "-", Day, " ", Hour))+hours(adjustment),
         y_day = yday(Date),
         Hour = hour(Date),
         Year = year(Date)) %>% 
  filter(Year==year) %>% 
  filter(y_day <= 5) %>% #smaller sample for testing
  group_by(y_day, Hour, Longitude, Latitude) %>% # variables to keep 
  summarise(precipitation = mean(precipitation, na.rm = TRUE), # gets rid of duplicate hour reports for a station
            temperature = mean(temperature, na.rm = TRUE),
            snow_depth = max(snow_depth)) %>% # max makes sense for depth right? 
  mutate_all(~ifelse(is.nan(.), NA, .)) %>% 
  st_as_sf(coords = c("Longitude", "Latitude"), crs=4326) %>% # these are in crs 4326, so need to load, then convert 
  st_transform(crs = projection) 

# Check to make sure it's working correctly
# ggplot() +
#   geom_sf(data = timezone_adj) +
#   geom_sf(data = wx)

if(max(as.numeric(training_frame_rc$Hour), na.rm=TRUE)==24){ # swap from 1-24 to 0-23
  training_frame_rc <- training_frame_rc %>% rename(Hour_del = Hour) %>%
    mutate(Hour = as.numeric(Hour_del)-1) %>%
    select(-Hour_del)
}

# Daily Data Prep ---------------------------------------------------------
year <- 2021 # need 2020 data for daily
file_list <- list.files(path = file.path(inputdir, "Weather","GHCN", "Daily", year), pattern = paste0("^", state))
df_list <- list()
df_list = vector("list", length = length(file_list))

for(i in 1:length(file_list)){
  print(i)
  df_list[[i]] <- fread(file.path(inputdir, "Weather","GHCN", "Daily", year, file_list[[i]]))
}
state_daily_hist_weather <- do.call(bind_rows, df_list)

xy <- state_daily_hist_weather %>% 
  st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs=4326) %>% # these are in crs 4326, so need to load, then convert
  st_transform(crs = projection) %>% 
  mutate(Date = ymd(DATE),
         y_day = yday(Date)) %>% 
  dplyr::filter(y_day<=5) %>% # remove y_day argument when doing everything
  select(y_day, SNOW)
  



n = as.numeric(length(unique(wx$y_day))) * as.numeric(length(unique(wx$Hour)))
data_list <- list()
data_list = vector("list", length = n)


# These are to test without the loop
# x <- 1
# y <- 1

Sys.time()

for(x in 1:length(unique(wx$y_day))){
  for(y in 0:(length(unique(wx$Hour))-1)){ 
    roads <- training_frame_rc %>% filter(as.numeric(Day) == x & as.numeric(Hour) == y) # road filter
    
    # precipitation 
    precip <- wx %>% filter(y_day == x & Hour == y & !is.na(precipitation)) %>% 
      select(precipitation) 
    
    if(nrow(precip) == 0){
      working_df <- roads %>% mutate(precipitation = 0) # if all weather stations were NA, set to zero
    } else{
      working_df <- roads %>% st_join(precip, join=st_nearest_feature) # if there is weather station data, nearest join 
    }
    
    # temperature 
    temp <- wx %>% filter(y_day == x & Hour == y & !is.na(temperature)) %>% 
      select(temperature) 
    
    if(nrow(temp) == 0){
      working_df <- working_df %>% mutate(temperature = NA) # if all weather stations were NA, set to NA (unique for temp)
    } else{
      working_df <- working_df %>% st_join(temp, join=st_nearest_feature) # if there is weather station data, nearest join 
    }
    
    # snow 
    # snow <- wx %>% filter(y_day == x & Hour == y & !is.na(snow_depth)) %>% 
    #   select(snow_depth) 
    # 
    # if(nrow(snow) == 0){
    #   working_df <- working_df %>% mutate(snow_depth = 0) # if all weather stations were NA, set to zero
    # } else{
    #   working_df <- working_df %>% st_join(snow, join=st_nearest_feature) # if there is weather station data, nearest join 
    # }

    data_list[[(x*(y+1))]] <- working_df
      
  }

}

training_frame_rcw <- do.call(bind_rows, data_list) # more efficient 

n = as.numeric(length(unique(wx$y_day)))
data_list <- list()
data_list = vector("list", length = n)

# Daily Snow 
for(x in 1:length(unique(xy$y_day))){
  roads <- daily_for_snow %>% filter(as.numeric(Day) == x)
  snow <- xy %>% filter(y_day == x & !is.na(SNOW)) %>% select(SNOW)

  if(nrow(snow) == 0){
    working_df <- roads %>% mutate(SNOW = 0) %>% st_drop_geometry() # if all weather stations were NA, set to zero
  } else{
    working_df <- roads %>% st_join(snow, join=st_nearest_feature) %>% st_drop_geometry() # if there is weather station data, nearest join
  }
  
  data_list[[x]] <- working_df
}

daily_merge <- do.call(bind_rows, data_list)

training_frame_rcws <- training_frame_rcw %>% left_join(daily_merge, by=c("osm_id", "Day"))

ggplot() + 
  geom_sf(data = working_df, aes(color = temperature)) + 
  scale_color_gradient(low = "blue", high = "red")
  