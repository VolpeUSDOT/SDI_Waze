# Visualize next week predictions TN
# Relies on following objects:
# next_week_out: Created in PredictWeek_TN.R
# g: grid name, string
#

censusdir <- "~/TN/Input/census"
proj.USGS <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"

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

pdf(paste0('Crash_prob_', g, "_", Sys.Date(),'.pdf'),
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
  