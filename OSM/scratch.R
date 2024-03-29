library(ggplot2)
library(tigris)
library(sf)

setwd(file.path(dirname(rstudioapi::getActiveDocumentContext()$path)))


load_hpms <- function(states = "All", # includes all HPMS segments by default including DC and PR, though not recommended.
                      year = "2018"){ # 2018 is more recent data as of writing this 
  suppressWarnings(if(states == "All"){ 
    list <- append(state.name, "District")
    list <- append(list, "PuertoRico")
  }
  else{
    list <- states
  })
  s = 0
  for(x in list){
    s = s + 1
    link <- paste0("https://geo.dot.gov/server/rest/services/Hosted/", gsub(" ", "", x, fixed = TRUE), "_", year, "_PR/FeatureServer/0/query?returnGeometry=true&where=1=1&outFields=*&f=geojson")
    print(link)
    working_df <- st_read(link)
    if(s == 1){
      df <- working_df
    }
    else{
      df <- df %>% rbind.fill(working_df)
    }
  }
  return(df)
}

state <- 'Minnesota'

state_maps <- states(cb = TRUE, year = 2021) %>%
  filter_state(state) %>%
  st_transform(crs = 'WGS84') 

minnesota_network <- load_hpms(states = state, year = '2018')

crashes <- read_sf('Washington State/wa21crash.shp')

ggplot() + 
  geom_sf(data = crashes, aes(color = 'Crash', shape = 'Crash'), size = .05) +
  geom_sf(data = Washington_State_network, aes(color = 'Network', shape = 'Network'), alpha = 1) +
  geom_sf(data = state_maps, aes(color = 'Border', shape = 'Border'), linetype = 'dashed', fill = NA, alpha = 1) +
  scale_color_manual(values = c('Crash' = 'blue', 'Network' = 'black', 'Border' = 'black')) +
  scale_shape_manual(values = c('Crash' = 16, 'Network' = 1, 'Border' = 2)) +
  guides(color = guide_legend(override.aes = list(shape = c(16, 1, 2), linetype = c("dashed", "solid", "solid")), title = "Layer"),
         shape = guide_legend(title = "Layer"))  +
  theme_minimal() +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())

write_sf(state_maps, file.path(state, paste0(state,'_border.shp')))
