# Info --------------------------------------------------------------------
# Purpose: Merges hourly weather data with road segments. 
# Author: Joey Reed (joseph.reed1@dot.gov)
# Last edited: 3/25/2024

# Notes: 
# Some crazy high numbers for precip and temp. 
# Will need to consider how we are handling the timezone factor. Simply adjusting the time zone will leave a couple hours without data. 

# Prep --------------------------------------------------------------------

# directory 
inputdir <- file.path(getwd(), "Input")

# Load packages 
library(tidyverse)
library(sf)
library(data.table) # contains FREAD

# Projection 
projection <- 5070 

# Hourly Data Prep --------------------------------------------------------

# Read in list of GHCN hourly stations
stations <- read.csv(file=file.path(inputdir, "Weather","GHCN", "Hourly","ghcnh-station-list.csv"), 
                     header = FALSE, strip.white = TRUE)
stations <- stations[,1:6]
colnames(stations) <- c('Station_ID','Latitude','Longitude','Elevation','State','Station_name') 

# Filter down to the list of stations in the state of interest
state_stations <- stations[stations$State==state,]

# Filter down to the list of stations for which we have hourly data in the year of interest
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


# Daily Data Prep ---------------------------------------------------------
year <- 2021 # no 2019 data, so need to test with 2021
file_list <- list.files(path = file.path(inputdir, "Weather","GHCN", "Daily", year), pattern = paste0("^", state))
df_list <- list()
df_list = vector("list", length = length(file_list))

for(i in 1:length(file_list)){
  df_list[[i]] <- fread(file.path(inputdir, "Weather","GHCN", "Daily", year, file_list[[i]]))
}
state_daily_hist_weather <- do.call(bind_rows, df_list)

xy <- state_daily_hist_weather %>% 
  mutate(Date = ymd(DATE),
         y_day = yday(Date)) %>% 
  dplyr::filter(y_day<=7) %>% # remove y_day argument when doing everything
  st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs=4326) %>% # these are in crs 4326, so need to load, then convert
  st_transform(crs = st_crs(projection)) %>% 
  select(y_day, SNOW)
  

# data processing 
wx <- state_hourly_hist_weather %>% 
  dplyr::filter(Month == 1 & Day <= 7) %>% # remove this filter when doing all of it 
  mutate(Date = ymd_h(paste0(Year, "-", Month, "-", Day, " ", Hour)),
         y_day = yday(Date)) %>% 
  group_by(y_day, Hour, Longitude, Latitude) %>% # variables to keep 
  summarise(precipitation = mean(precipitation, na.rm = TRUE), # gets rid of duplicate hour reports for a station
            temperature = mean(temperature, na.rm = TRUE),
            snow_depth = max(snow_depth)) %>% # max makes sense for depth right? 
  mutate_all(~ifelse(is.nan(.), NA, .)) %>%
  st_as_sf(coords = c("Longitude", "Latitude"), crs=4326) %>% # these are in crs 4326, so need to load, then convert 
  st_transform(crs = st_crs(projection))

if(max(as.numeric(training_frame_rc$Hour), na.rm=TRUE)==24){ # swap from 1-24 to 0-23
  training_frame_rc <- training_frame_rc %>% rename(Hour_del = Hour) %>%
    mutate(Hour = as.numeric(Hour_del)-1) %>%
    select(-Hour_del)
}

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
  snow <- xy %>% filter(y_day == x) %>% select(SNOW)

  if(nrow(snow) == 0){
    working_df <- roads %>% mutate(SNOW = 0) %>% st_drop_geometry() # if all weather stations were NA, set to zero
  } else{
    working_df <- roads %>% st_join(snow, join=st_nearest_feature) %>% st_drop_geometry() # if there is weather station data, nearest join
  }
  
  data_list[[x]] <- working_df
}

daily_merge <- do.call(bind_rows, data_list)

training_frame_rcws <- training_frame_rcw %>% left_join(daily_merge, by=c("osm_id", "Days"))

ggplot() + 
  geom_sf(data = working_df, aes(color = temperature)) + 
  scale_color_gradient(low = "blue", high = "red")
  