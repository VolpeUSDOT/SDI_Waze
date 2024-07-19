
# Beginning here copied from osm_query.R, but removing the weather part.

# Background ----------------------------

library(dplyr)
library(osmdata)
library(sf)
library(ggplot2)
library(tigris)
library(doParallel)
library(lubridate)
library(stringr)

rm(list=ls()) # clear enviroment

outputdir <- file.path(getwd(),"Output")
inputdir <- file.path(getwd(),"Input")
intermediatedir <- file.path("Intermediate")

# make directories if not already there
if(!dir.exists(outputdir)){dir.create(outputdir)}
if(!dir.exists(intermediatedir)){dir.create(intermediatedir)}
if(!dir.exists(file.path(intermediatedir,'CAD_Waze'))){dir.create(file.path(intermediatedir,'CAD_Waze'))}

# Identify state for analysis; Need to specify Washington State for Washington
state <- "MN" 

state_osm <- ifelse(state == "WA", "Washington State",
                    ifelse(state == "MN", "Minnesota", NA))

state_osm <- gsub(" ", "_", state_osm) # Normalize state name

# Projection 
projection <- 5070 

# Year
year <- 2020

# Load Road Data ----------------------

##select server to query OSM api
#If the below does not work, try https://overpass.kumi.systems/api/interpreter

new_url <- "https://overpass-api.de/api/interpreter"
set_overpass_url(new_url)

##identify road types you want to query for; can pick from c(motorway, trunk, primary, secondary, tertiary, unclassified, residential)
road_types <- c("motorway", "trunk", "primary", "secondary", "tertiary")

#initialize object
roadways <- list()

network_file <- paste0(state_osm,"_network")
boundary_file <- paste0(state_osm,"_boundary")
file_path <- file.path(inputdir,"Roads_Boundary", state_osm, paste0(network_file, ".gpkg"), paste0(network_file,".shp"))

if (file.exists(file.path(file_path))){
  state_network <- read_sf(file_path)
  print("File Found")
} else{
  
  n = as.numeric(length(road_types)) 
  datalist <- list()
  datalist = vector("list", length = n)
  state_bbox <- getbb(state_osm) # Retrieves relevant coordinates; always a rectangle
  l = 0
  
  for (i in road_types){
    l = l + 1 
    print(paste("File Not Found. Pulling", state, i, "OSM Data from Server"))
    map_data <- opq(bbox = state_bbox) %>%
      add_osm_feature(key = 'highway', value = i) %>%
      osmdata_sf() 
    lines <- map_data$osm_lines
    
    datalist[[l]] <- lines
    
  }
  
  total_network <- do.call(bind_rows, datalist)
  
  # if (!(dir.exists(state_osm))){
  #   dir.create(state_osm)}
  
  total_network <- st_transform(total_network, crs = projection) 
  
  # Pull state boundaries
  if (state_osm == 'Washington_State'){
    state_border <- 'Washington'
  }else{
    state_border <- state_osm
  }
  state_maps <- states(cb = TRUE, year = 2021) %>%
    filter_state(state_border) %>%
    st_transform(crs = projection)
  
  #Filter out roadways outside the state
  
  state_network <- st_join(total_network, state_maps, join = st_within) %>%
    filter(!is.na(NAME)) %>%
    select(osm_id, highway, ref, geometry)
  
  write_sf(state_network, file.path(inputdir,'Roads_Boundary', state_osm, paste0(network_file, '.gpkg')), driver = "ESRI Shapefile")
  write_sf(state_border, file.path(inputdir,'Roads_Boundary', state_osm, paste0(boundary_file, '.gpkg')), driver = "ESRI Shapefile")
  rm(state_border)
}

#ggplot() + geom_sf(data = state_network)

#Transform state_network crs to the projection
if(st_crs(state_network) != projection){
  state_network <- st_transform(state_network, projection)
}

####Read in CAD data for 2020
CAD20 <- read.csv(file.path(inputdir,"Crash","2020_mndot_cadevents.csv")) %>%
  mutate(lat=as.numeric(lat),
         lon=as.numeric(lon),
         centraltime=as.POSIXct(open,format="%m/%d/%Y %H:%M",tz="America/Chicago"),
         close = as.POSIXct(close,format="%m/%d/%Y %H:%M",tz="America/Chicago"),
         time_open = difftime(close, centraltime, units = "mins"),
         month = lubridate::month(centraltime)) %>%
  # some of the lat and lon values had the string "None," so above function resulted in NAs by coercion.
  # filter them out now
  filter(!is.na(lon)&!is.na(lat)) %>%
  # filter out also the rows that were duplicates, mistakes, or 'unable to locate' based on police or traffic reports
  filter(!p_disposition %in% c('DUPNCAN','UTL','CE','DUP','CANCELEV')) %>%
  filter(!t_disposition %in% c('DUPNCAN','UTL','CE','DUP','CANCELEV')) %>%
  # filter out also the rows where the time_open is only a few minutes. According to MnDOT,  
  # those are likely erroneous because they were closed so soon after being opened.
  filter(time_open > 5) %>%
  # select columns of interest
  dplyr::select(pkey, eid, class, lat, lon, centraltime, time_open, location, month) %>%
  # convert to sf object
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(projection) %>%
  # associated each record with the road segment it is nearest to
  st_join(state_network, join = st_nearest_feature)

# Create a function that looks for potential matches in a second dataset (sf object) and adds info to a first dataset
# (sf object) based on them.
find_matches <- function(data1_sf, 
                         data2_sf,
                         time1,
                         time2,
                         ID1,
                         ID2,
                         near_dist = 0.5,
                         near_time = 60
){
  data1_df <- data1_sf %>% st_drop_geometry()
  data2_df <- data2_sf %>% st_drop_geometry()
  
  for(r in 1:nrow(data1_sf)){
    # compute distances from this crash point to every other point in the other dataset
    # use 'as.numeric' to remove units and then conver from meters to miles by multiplying by 0.0006213712
    distances = as.numeric(st_distance(data1_sf[r,],data2_sf))*0.0006213712
    # compute time differences
    time_diffs <- as.numeric(difftime(rep(data1_df[r,time1],nrow(data2_df)),data2_df[,time2], units = "mins"))
    # define possible matches as those within the thresholds
    matches <- (distances <= near_dist) & (abs(time_diffs) <= near_time)
    # what is the total nmbe rof matches for htis point in the first dataset?
    data1_sf[r,"matches"] <- sum(matches)
    # what are the IDs in the second dataset for the asssociated time differences and distances?
    t_d_w_ids <- data.frame(ID = data2_df[,ID2], time_diffs = time_diffs, distances = distances)
    # if at least one match, inssert the ID, time difference ,and distance of the "bset" match in
    # the dataframe for hte first dataset, where "best" is based on minimum time difference.
    if(sum(matches)>0){
      min_time_diff_for_matches <- min(t_d_w_ids[matches, "time_diffs"])
      is_min <- t_d_w_ids$time_diffs == min_time_diff_for_matches
      # if tehre is a tie, take hte first on
      data1_sf[r,"best_m_ID"] <- t_d_w_ids[is_min,"ID"][1]
      data1_sf[r,"best_m_time"] <- t_d_w_ids[is_min,"time_diffs"][1]
      data1_sf[r,"best_m_dis"] <- t_d_w_ids[is_min,"distances"][1]
      
    } # end if loop to add details for "best match," if applicable
  } # end for loop to look for potential matches in the second dataset for each point and append to the first absed on them
  return(data1_sf)
} # end function


# establish set of waze files that we'll be reading in
waze.files <- dir(file.path(inputdir, "Waze", state))

waze.files.year <- file.path(inputdir, "Waze", state, waze.files[grep(as.character(year), waze.files)])

#m <- 1
gc()
starttime = Sys.time()
for(m in 1:12){
  # read in data frames for that month
  waze.month <- read.csv(waze.files.year[m]) %>% 
    filter(alert_type =="ACCIDENT") %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    st_transform(projection) %>%
    st_join(state_network, join = st_nearest_feature) %>% 
    # convert to date-time
    mutate(time_local = as.POSIXct(time_local, format = "%Y-%m-%d %H:%M:%S", tz = "America/Chicago"))
  waze.month$id <- paste0(formatC(m, width = 2, flag = "0"), "_", 1:nrow(waze.month))
  
  CAD.month <- CAD20 %>% filter(month == m) %>% filter(class == "CRASH")
  
  #call the find matches function on each and save the result, then clear from memory before doing the next month
  CAD.month <- find_matches(data1_sf = CAD.month,
                            data2_sf = waze.month,
                            time1 = "centraltime",
                            time2 = "time_local",
                            ID1 = "eid",
                            ID2 = "id",
                            near_dist = 0.5,
                            near_time = 60)
  
  waze.month <- find_matches(data1_sf = waze.month,
                            data2_sf = CAD.month,
                            time1 = "time_local",
                            time2 = "centraltime",
                            ID1 = "id",
                            ID2 = "eid",
                            near_dist = 0.5,
                            near_time = 60)
  save(list = c('CAD.month','waze.month'), file = file.path(intermediatedir, 'CAD_Waze', paste0("month_", m, ".RData")))
  
  rm(list = c('CAD.month','waze.month'))
  
  timediff = Sys.time() - starttime
  
  cat("Completed month ", m, "...   ")
  cat(round(timediff, 2), attr(timediff, "unit"), "elapsed", "\n")
}

# Finally, read in and combine all the files before summary analysis and visuals
CADfull <- data.frame()
wazefull <- data.frame()

for(m in 1:12){
  load(file.path(intermediatedir, 'CAD_Waze', paste0("month_", m, ".RData")))
  CADfull <- rbind(CADfull, CAD.month)
  wazefull <- rbind(wazefull, waze.month)
  rm(list = c('CAD.month','waze.month'))
  cat("Loaded month ", m, "...  ")
}

boundary_file <- paste0(state_osm, "_boundary")
boundary_file_path <- file.path(inputdir, 'Roads_Boundary', state_osm, paste0(boundary_file, '.gpkg'), paste0(state_osm, '_border.shp'))

if (file.exists(file.path(boundary_file_path))){
  state_boundary <- read_sf(boundary_file_path)
  print("File found")
} else {print("No state boundary file.")}

state_boundary <- state_boundary %>% st_transform(crs = projection)

counties <- read_sf(file.path(inputdir, "Roads_Boundary", state_osm, "shp_bdry_counties_in_minnesota","mn_county_boundaries.shp")) %>%
  st_transform(crs = projection)

# summarize by county
CADfull <- st_join(CADfull, counties %>% select(CTY_FIPS), join = st_nearest_feature)

CADfull$matched <- ifelse(CADfull$matches > 0, 1, 0)

by_county <- CADfull %>% group_by(CTY_FIPS) %>% summarize(CAD_match_percentage = mean(matched)*100) %>% st_drop_geometry()

counties <- counties %>% left_join(by_county, by = join_by(CTY_FIPS == CTY_FIPS))

# same for Waze in the opposite direction
wazefull <- st_join(wazefull, counties %>% select(CTY_FIPS), join = st_nearest_feature)

wazefull$matched <- ifelse(wazefull$matches > 0, 1, 0)

by_county_w <- wazefull %>% group_by(CTY_FIPS) %>% summarize(waze_match_percentage = mean(matched)*100) %>% st_drop_geometry()

counties <- counties %>% left_join(by_county_w, by = join_by(CTY_FIPS == CTY_FIPS))

CADmap <- ggplot() +
  geom_sf(data = state_boundary, aes(), linetype = 'solid', linewidth = 2, fill = NA, alpha = 1) +
  geom_sf(data = counties, aes(fill = CAD_match_percentage), alpha = 0.5, size = 0.5) +
  labs(title = "Percentage of CAD Crash Records with a \nWaze Match (by County)") +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())

ggsave(file.path(outputdir, "CADmap2020.png"), CADmap)

wazemap <- ggplot() +
  geom_sf(data = state_boundary, aes(), linetype = 'solid', linewidth = 2, fill = NA, alpha = 1) +
  geom_sf(data = counties, aes(fill = waze_match_percentage), alpha = 0.5, size = 0.5) +
  labs(title = "Percentage of Waze Crash Records with a \nCAD Match (by County)") +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())

ggsave(file.path(outputdir, "Wazemap2020.png"), wazemap)

# Load HSIS crash data for 2020
crash_files <- list.files(file.path(inputdir, "Crash", state_osm), pattern = 'crash.shp$', full.names = TRUE)

timestamps <- read.csv(file.path(inputdir, 'Crash', state_osm, paste0(state_osm, '_timestamps.csv'))) %>%
  rename(INCIDEN = INCIDENT_ID) %>%
  mutate(centraltime = as.POSIXct(DATE_TIME_OF_INCIDENT, format = "%m/%d/%Y %H:%M", tz = "America/Chicago"),
         month = lubridate::month(centraltime))

HSIS20 <- read_sf(crash_files[5]) %>%
                    left_join(timestamps, by = 'INCIDEN') %>%
                    select(INCIDEN, centraltime, month) %>%
                    st_transform(projection) %>%
                    st_join(state_network, join = st_nearest_feature)


#### BELOW IS JUST NOTES #####

# for MN, the first 5 crash shapefiles were originally in WGS84 coordinate reference system (4326)
# and the 6th shapefile was in UTM zone 15N coordinate reference system (4269)...
# for CAD we'll have to assume that the coordinates are in 4326

