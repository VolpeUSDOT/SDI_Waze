# Visualize TN crashes for TRR paper

# Setup ----

library(rgeos)
library(rgdal)
library(sp)
library(tidyverse)
library(ggpubr)

inputdir <- "~/TN/Input"
censusdir <- "~/TN/Input/census"
outputdir<- "~/TN/Output"
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

g = "TN_01dd_fishnet" # or "TN_1sqmile_hexagons"

# Simple corr plot ----

d = read_csv(file.path(outputdir, 'TN_2017-04_to_2018-03_TN_1sqmile_hexagons.csv'))

# Plot correlation between total TN crashes and Waze crash reports by day

d_day <- d %>%
  group_by(year, day) %>%
  summarize(TNcrash = sum(nTN_total),
            Wazecrash = sum(nWazeAccident))

ggplot(d_day, aes(x = TNcrash, y = Wazecrash)) +
  geom_point() + 
  theme_bw() +
  ylab('Count of Waze crash reports') + xlab('Count of Police crash reports') +
  ggtitle('Police- and Waze-reported crashes in Tennessee \n Statewide daily totals (2017-04 to 2018-03)') +
#  ylim(0, 1500) + xlim(0, 1500) +
#  geom_smooth(method = 'lm') +
  stat_cor(method = "pearson", label.x = 3, label.y = 1500)

ggsave(file.path(outputdir, 'PearsonCorr_TN_Waze.jpeg'), width = 6, height = 6)

# Mapping ---- 

grid_shp <- rgdal::readOGR(file.path(inputdir, "Shapefiles"), layer = g)
grid_shp <- spTransform(grid_shp, CRS(proj.USGS))

# Read in buffered state shapefile
tn_buff <- readOGR(censusdir, layer = "TN_buffered")
tn_buff <- spTransform(tn_buff, CRS(proj.USGS))

# Clip grid to county shapefile
grid_intersects <- gIntersects(tn_buff, grid_shp, byid = T)

grid_shp <- grid_shp[as.vector(grid_intersects),]

plotgrid <- grid_shp

# Plot by day, aggregated 
pred_dat <- next_week_out %>%
  group_by(GRID_ID, date.x) %>%
  summarize(Crash_pred = sum(as.numeric(Crash_pred)),
            Prob_NoCrash = max(Prob_NoCrash),
            Prob_Crash = max(Prob_Crash))

pred_dat$GRID_ID <- as.character(pred_dat$GRID_ID)

pdf(file.path(outputdir,"Figures",paste0('Crash_prob_', g, "_", Sys.Date(),'.pdf')),
    width = 8, height = 5)

for(day in as.character(unique(pred_dat$date.x))){
  plotgrid <- grid_shp
  
  plotgrid@data <- left_join(plotgrid@data, pred_dat %>% filter(date.x == day), by = "GRID_ID")
  
  plotgrid@data[plotgrid@data==-Inf] = NA
  
  # Make crash probability maps
  n_colors = 5
  probcol <- rev(heat.colors(n_colors))
  
  cuts = cut(plotgrid@data$Prob_Crash, n_colors)
  
  plot(plotgrid, col = probcol[cuts], 
       border =  probcol[cuts],
       bg = 'grey20')
  
  legend("bottom", 
         #bty = 'n',
         fill = probcol,
         legend = c('Very low',
                    'Low',
                    'Medium',
                    'High',
                    'Very high'),#levels(cuts),
         cex = 0.8, ncol = 3, pt.cex = 2,
         bg = 'grey60')
  title(main = paste("Crash probabilities on", as.character(day)))
}  
dev.off()
  