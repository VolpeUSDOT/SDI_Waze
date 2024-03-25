# Info --------------------------------------------------------------------
# Purpose: Merges hourly weather data with road segments. 
# Author: Joey Reed (joseph.reed1@dot.gov)
# Last edited: 3/25/2024

# Notes: 
# Some crazy high numbers for precip and temp. 

# Prep --------------------------------------------------------------------

# Clear 
rm(list=ls())

# Load packages 
library(tidyverse)
library(sf)

# Directories
censusdir <- file.path(getwd(),"Input","census")
outputdir <- file.path(getwd(),"Output") # to hold daily output files as they are generated
inputdir <- file.path(getwd(),"Input")

# Define Variables 
state <- 'TN'
year <- 2019
projection <- 9311 # equal area best for nearest neighbor calc 



# Load in Data -------------------------------------------------------
# This script will likely get referenced and will not need this seciton in future. 
road_network <- read_sf(file.path(inputdir, "shapefiles", "TN_daily", "jan_01_hourly_TN_network.shp"))
road_network <- st_transform(road_network, crs=projection)


# Read in list of GHCN hourly stations
stations <- read.csv(file=file.path(inputdir, "Weather","GHCN","Hourly","ghcnh-station-list.csv"), 
                     header = FALSE, strip.white = TRUE)
stations <- stations[,1:6]
colnames(stations) <- c('Station_ID','Latitude','Longitude','Elevation','State','Station_name') 

# Filter down to the list of stations in the state of interest
state_stations <- stations[stations$State==state,]

# Filter down to the list of stations for which we have data in the year of interest
available <- logical(nrow(state_stations))
for (i in 1:nrow(state_stations)) {
  ID = state_stations[i,'Station_ID']
  filename = paste0('GHCNh_',ID,'_',year,'.psv')
  print(filename) # check what it is checking
  ifelse(file.exists(file.path(inputdir, "Weather", "GHCN", "Hourly", year, state, filename)
  ),
  available[i] <- TRUE,
  available[i] <- FALSE
  )
}
state_stations <- state_stations[available,]

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
for (i in 1:nrow(state_stations)){
  ID = state_stations[i,'Station_ID']
  filename = paste0('GHCNh_',ID,'_',year,'.psv')
  df_i = read.table(file.path(inputdir, "Weather", "GHCN", "Hourly", year, state, filename),
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



# data processing 

wx <- state_hourly_hist_weather %>% 
  dplyr::filter(Month == 1 & Day == 1) %>% 
  mutate(Date = ymd_h(paste0(Year, "-", Month, "-", Day, " ", Hour)),
         y_day = yday(Date)) %>% 
  group_by(y_day, Hour, Longitude, Latitude) %>% # variables to keep 
  summarise(precipitation = mean(precipitation, na.rm = TRUE), # gets rid of duplicate hour reports for a station
            temperature = mean(temperature, na.rm = TRUE),
            snow_depth = max(snow_depth)) %>% # max makes sense for depth right? 
  mutate_all(~ifelse(is.nan(.), NA, .)) %>%
  st_as_sf(coords = c("Longitude", "Latitude"), crs=4326) %>% # these are in crs 4326, so need to load, then convert 
  st_transform(crs = st_crs(projection))

temp_empty_weather <- wx %>% slice(0) %>% select(-y_day, -Hour) %>% st_drop_geometry()
weather_merged <- road_network %>% slice(0) %>% bind_cols(temp_empty_weather)

# DELETE THESE ONCE FOR LOOP IS TESTED
# x <- 1
# y <- 1

Sys.time()

for(x in 1:length(unique(wx$y_day))){
  for(y in 0:(length(unique(wx$Hour))-1)){ 
    roads <- road_network %>% filter(as.numeric(Day) == x & as.numeric(Hour) == y) # road filter
    
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
    snow <- wx %>% filter(y_day == x & Hour == y & !is.na(snow_depth)) %>% 
      select(snow_depth) 
    
    if(nrow(snow) == 0){
      working_df <- working_df %>% mutate(snow_depth = 0) # if all weather stations were NA, set to zero
    } else{
      working_df <- working_df %>% st_join(snow, join=st_nearest_feature) # if there is weather station data, nearest join 
    }
    
    weather_merged <- weather_merged %>% bind_rows(working_df) # binding together the resutls 
      
  }
}

Sys.time() # took 1 minute and 15 seconds for Jan 1st in TN 

ggplot() + 
  geom_sf(data = working_df, aes(color = temperature)) + 
  scale_color_gradient(low = "blue", high = "red")
  
