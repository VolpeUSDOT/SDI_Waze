
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

###### establish set of waze files that we'll be reading in
waze.files <- dir(file.path(inputdir, "Waze", state))

waze.files.year <- file.path(inputdir, "Waze", state, waze.files[grep(as.character(year), waze.files)])

waze_jan <- read.csv(waze.files.year[1])
unique(waze_jan$sub_type)

source('MN_CAD_analysis/CAD_Waze_functions.R')

gc()
crash_match_files <- get_matches_by_type(CADtypes = "CRASH", Wazetypes = "ACCIDENT")
crash_match_CAD <- crash_match_files$CADfull
crash_match_waze <- crash_match_files$wazefull
rm(crash_match_files)
save(list = c("crash_match_CAD", "crash_match_waze"), file = file.path(getwd(),'MN_CAD_Analysis','CAD_Waze_matches.Rdata'))

gc()
parked_veh_match_files <- get_matches_by_type(CADtypes = c("STALL"), 
                                              Wazetypes = c("HAZARD_ON_SHOULDER_CAR_STOPPED", "HAZARD_ON_ROAD_CAR_STOPPED"),
                                              sub_or_alert = sub_type)
parked_veh_match_CAD <- parked_veh_match_files$CADfull
parked_veh_match_waze <- parked_veh_match_files$wazefull
rm(parked_veh_match_files)

unique(CAD20$class)
unique(crash_match_waze$alert_type)
unique(crash_match_waze$sub_type)

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

save(list = c('CAD20','CADfull','wazefull'), file = file.path(getwd(),'MN_CAD_Analysis','CAD_Waze_matches.Rdata'))

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

# View separately by road type
by_roadtype_CAD <- CADfull %>% group_by(highway) %>% summarize(match_percentage = mean(matched)*100) %>% st_drop_geometry()

by_roadtype_Waze <- wazefull %>% group_by(highway) %>% summarize(match_percentage = mean(matched)*100) %>% st_drop_geometry()

ggplot(by_roadtype_CAD) + 
  geom_col(aes(x = highway, y = match_percentage)) +
  labs(title = "Percentage of CAD Crash Records with a \nWaze Match (by Road Class)")  +
  theme_minimal()

ggplot(by_roadtype_Waze) + 
  geom_col(aes(x = highway, y = match_percentage)) +
  labs(title = "Percentage of Waze Crash Records with a \nCAD Match (by Road Class)")  +
  theme_minimal()


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

