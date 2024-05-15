
#Identifies and creates a df of the crashes that may have been secondary crashes
potential_secondary_crashes <- joined_sf %>%
  group_by(Year, Day, Hour, osm_id) %>%
  mutate(crashes = n()) %>%
  filter(crashes > 1) 

#Identifies the OSM links that had secondary crashes
secondary_links <- joined %>%
  filter(crash > 1) %>%
  left_join(state_network, by = 'osm_id') %>%
  st_as_sf() 

sum(secondary_links$crash)

##Buffer Approach

##Identify distance threshold in meters
buffer_distance <- 100

##Creates a buffer zone around all crash points
joined_buffer <- st_buffer(joined_sf, dist = buffer_distance)

##Systematically foes through each hour of each day and identifies each crash within 100 meters if another one
temp <- data.frame()
for (d in unique(joined$Day)){
  for (h in unique(joined$Hour)){
    temp_buffer <- joined_buffer %>%
      filter(Day == d) %>%
      filter(Hour == h)
    temp_norm <- joined_sf %>%
      filter(Day == d) %>%
      filter(Hour == h)
    
    grouped <- st_join(temp_norm, temp_buffer, join = st_within) %>% 
      filter(!(CASENO.x == CASENO.y)) #to prevent a crash matching with itself
    
    temp <- rbind(temp, grouped)
  }
}

#Resulting dataframe is messy but was meant to give a ballpark estimate of potential secondary crashes