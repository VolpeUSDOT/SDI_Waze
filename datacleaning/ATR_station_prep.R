# ATR counters

# weigh in motion stations
# https://hifld-geoplatform.opendata.arcgis.com/datasets/weigh-in-motion-stations/data

# traffic volume trends
# https://www.fhwa.dot.gov/policyinformation/travel_monitoring/tvt.cfm

library(tidyverse)

# Stations from ATR
setwd('//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Data/ATR')

codes16 <- list(
  record_type = 1, # "Record Type"
  fips_state = 2, # "FIPS State Code"
  station_id = 6, # "Station ID"
  direction = 1, # "Direction of Travel Code"
  lane = 1, # "Lane of Travel"
  year = 2, # "Year of Data"
  func_clas = 2, # "Functional Classification Code"
  through_lane = 1, # "Number of Lane, # in Direction Indicated"
  sample_type_volume = 1, # "Sample Type for Traffic Volume"
  lanes_monitored_volume = 1, # "Number of Lane, # Monitored for Traffic Volume"
  counting_method_volume = 1, # "Method of Traffic Volume Counting"
  sample_type_classification = 1, # "Sample Type for Vehicle Classification"
  lanes_monitored_calssification = 1, # "Number of Lane, # Monitored for Vehicle Class"
  counting_method_classification = 1, # "Method of Vehicle Classification"
  classification_algorithm = 1, # "Algorithm for Vehicle Classification"
  classification_system = 2, # "Classification System for Vehicle Classification"
  sample_type_weight = 1, # "Sample Type for Truck Weight"
  lanes_monitored_weight = 1, # "Number of Lane, # Monitored for Truck Weight"
  method_weight = 1, # "Method of Truck Weighing"
  calibration_weight = 1, # "Calibration of Weighing System"
  data_retrieval_method = 1, # "Method of Data Retrieval"
  sensor_type = 1, # "Type of Sensor"
  sensor_type_2 = 1, # "Second Type of Sensor"
  purpose = 1, # "Primary Purpose - NEW"
  LRA_id = 12, # "LRS Identification - NEW"
  LRS_location = 6, # "LRS Location Point - NEW"
  latitude = 8, # "Latitude - NEW'
  longitude = 9, # "Longitude - NEW"
  SHRP_id = 4, # "SHRP Site Identification - NEW"
  prev_station_id = 6, # "Previou, # Station ID"
  year_start = 2, # "Year Station Established"
  year_end = 2, # "Year Station Discontinued"
  fips_county = 3, # "FIPS County Code"
  sample_type_HPMS = 1, # "HPMS Sample Type"
  sample_id_HPMS = 12, # "HPMS Sample Identifier"
  nat_hw_sy = 1, # "National Highway System - NEW"
  signing_post = 1, # "Posted Route Signing"
  route_num_post = 8, # "Posted Signed Route Number"
  signing_conc = 1, # "Concurrent Route Signing"
  route_num_conc = 8, # "Concurrent Signed Route Number"
  location = 50 # "Station Location"
)

# d <- read.fwf('stations/2016/atr_sta_all_months_2016.txt',
#               width = unlist(codes),
#               col.names = names(codes),
#               n = 10)

codes18 <- list(
  record_type = 1, # "Record Type"
  fips_state = 2, # "FIPS State Code"
  station_id = 6, # "Station ID"
  direction = 1, # "Direction of Travel Code"
  lane = 1, # "Lane of Travel"
  year = 2, # "Year of Data" !!!! Says 4 in PDF, but only 2
  func_clas = 2, # "Functional Classification Code"
  through_lane = 1, # "Number of Lanes in Direction Indicated"
  sample_type_tmas = 1, # "Sample Type for TMAS"
  lanes_monitored_volume = 1, # "Number of Lanes Monitored for Traffic Volume"
  counting_method_volume = 1, # "Method of Traffic Volume Counting"
  lanes_monitored_classification = 1, # "Number of Lanes Monitored for Vehicle Class"
  counting_mechanism_classification = 1, # "Mechanism of Vehicle Classification"
  counting_method_classification = 1, # "Method of Vehicle Classification"
  classification_grouping = 2, # "Vehicle Classification groupings"
  lanes_monitored_weight = 1, # "Number of Lanes Monitored for Truck Weight"
  method_weight = 1, # "Method of Truck Weighing"
  calibration_weight = 1, # "Calibration of Weighing System"
  data_retrieval_method = 1, # "Method of Data Retrieval"
  sensor_type = 1, # "Type of Sensor"
  sensor_type_2 = 1, # "Second Type of Sensor"
  purpose = 1, # "Primary Purpose - NEW 
  LRA_id = 12, # "LRS Route ID" # says 60 in the documentation !!! 
  LRS_location = 8, # "LRS Location Point - NEW"
  latitude = 8, # "Latitude - NEW'
  longitude = 9, # "Longitude - NEW"
  lttp_site_id = 4, # LTTP Site Identification
  prev_station_id = 6, # "Previous Station ID"
  year_start = 2, # "Year Station Established"
  year_end = 2, # "Year Station Discontinued"
  fips_county = 3, # "FIPS County Code"
  sample_type_HPMS = 1, # "HPMS Sample Type"
  sample_id_HPMS = 12, # "HPMS Sample Identifier"
  nat_hw_sy = 1, # "National Highway System - NEW"
  signing_post = 2, # "Posted Route Signing"
  route_num_post = 8, # "Posted Signed Route Number"
  location = 50 # "Station Location" # 
)



# d <- read.fwf('stations/2018/TMAS2018.sta',
#               width = unlist(codes18),
#               col.names = names(codes18), n = 10)



d <- read.fwf('stations/2017/TMAS2017.sta',
              width = unlist(codes18),
              col.names = names(codes18))

# Problem: some rows include a tab spacing in teh sampel_id_HPMS. So need to specify a sep different from '\t' to be used internally

d <- read.fwf('stations/2017/TMAS2017.sta',
              width = unlist(codes18),
              col.names = names(codes18),
              skip = 1420,
              n = 5,
              sep = ",")

# But you also can't use a comma, because that is used as well! See line 3602 in 2017. So try \ instead
d <- read.fwf('stations/2017/TMAS2017.sta',
              width = unlist(codes18),
              col.names = names(codes18),
              skip = 3595,
              n = 10,
              sep = "\\")

#Not backslash either. See line 6998. So find and replace all backslash with forward slash, then proceed.
d <- read.fwf('stations/2017/TMAS2017.sta',
              width = unlist(codes18),
              col.names = names(codes18),
              skip = 6990,
              n = 8,
              sep = "\\")

# Full data
d <- read.fwf('stations/2017/TMAS2017.sta',
              width = unlist(codes18),
              col.names = names(codes18),
              sep = "\\")


# Some problems for sure. Lat and long don't all line up

count17 <- d %>%
  filter(!is.na(func_clas)) %>%
  group_by(fips_state, func_clas) %>%
  summarize(station_count = length(unique(station_id)))

c17_w <- count17 %>%
  pivot_wider(names_from =  func_clas,
              values_from = station_count)


c17_w$fips_state <- formatC(c17_w$fips_state, width = 2, flag = 0)

c17_w$state <- usmap::fips_info(c17_w$fips_state)$abbr
# 8791 stations in 2017

# 2018 ---
d18 <- read.fwf('stations/2018/TMAS2018.sta',
              width = unlist(codes18),
              col.names = names(codes18),
              sep = "\\")

count18 <- d18 %>%
  filter(!is.na(func_clas)) %>%
  group_by(fips_state, func_clas) %>%
  summarize(station_count = length(unique(station_id)))

c18_w <- count18 %>%
  pivot_wider(names_from =  func_clas,
              values_from = station_count)


c18_w$fips_state <- formatC(c18_w$fips_state, width = 2, flag = 0)

c18_w$state <- usmap::fips_info(c18_w$fips_state)$abbr

sum(count18$station_count)
# 8766 stations in 2018