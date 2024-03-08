library(ggplot2)

for_graph <- Minnesota_network %>%
  filter(highway == 'motorway' |
           highway == 'primary' |
           highway == 'secondary' |
           highway == 'tertiary'
         )
ggplot() + geom_sf(data = for_graph)

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

crash <- rash 

minnesota <- load_hpms(states = 'Minnesota', year = '2018')

ggplot() + 
  geom_sf(data = crash, aes(color = 'Crash', shape = 'Crash'), alpha = .5, size = .05) +
  geom_sf(data = minnesota, aes(color = 'Network', shape = 'Network')) +
  geom_sf(data = state_maps, aes(color = 'Border', shape = 'Border'), fill = 'transparent') +
  scale_color_manual(values = c('Crash' = 'blue', 'Minnesota' = 'black', 'Border' = 'black')) +
  scale_shape_manual(values = c('Crash' = 16, 'Minnesota' = 1, 'Border' = 2)) +
  theme_minimal() +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())
ggplot() + 
  geom_sf(data = crash, aes(color = 'Crash', shape = 'Crash'), alpha = .5, size = .05) +
  geom_sf(data = minnesota, aes(color = 'Network', shape = 'Network'), alpha = 1) + # Setting alpha to 1 for consistent appearance
  geom_sf(data = state_maps, aes(color = 'Border', shape = 'Border'), fill = 'transparent', alpha = 1) + # Setting alpha to 1 for consistent appearance
  scale_color_manual(values = c('Crash' = 'blue', 'Network' = 'black', 'Border' = 'black')) +
  scale_shape_manual(values = c('Crash' = 16, 'Network' = 1, 'Border' = 2)) +
  guides(color = guide_legend(override.aes = list(shape = c(16, 1, 2), title = "Layer"),
                              title = "Layer")) +
  theme_minimal() +
  theme(axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank())
ggplot() + 
  geom_sf(data = crash, aes(color = 'Crash', shape = 'Crash'), alpha = .5, size = .05) +
  geom_sf(data = minnesota, aes(color = 'Network', shape = 'Network'), alpha = 1) +
  geom_sf(data = state_maps, aes(color = 'Border', shape = 'Border'), linetype = 'dashed', fill = NA, alpha = 1) + # Setting fill to NA to remove fill
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


