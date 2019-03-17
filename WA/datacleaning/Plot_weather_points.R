# Plot weather data -- snipped called by Prep_HistoricalWeather_Segment.R


wx.avg.jan.T <- wx %>%
  group_by(STATION, lon, lat) %>%
  filter(mo == "01") %>%
  summarize(avgtempmax = mean(TMAX, na.rm=T),
            avgtempmin = mean(TMIN, na.rm=T)
  )
wx.jan.proj <- spTransform(SpatialPointsDataFrame(wx.avg.jan.T[c("lon", "lat")], 
                                                  wx.avg.jan.T,
                                                  proj4string = CRS("+proj=longlat +datum=WGS84")), CRS(proj.USGS))

wx.avg.jun.T <- wx %>%
  group_by(STATION, lon, lat) %>%
  filter(mo == "06") %>%
  summarize(avgtempmax = mean(TMAX, na.rm=T),
            avgtempmin = mean(TMIN, na.rm=T)
  )
wx.jun.proj <- spTransform(SpatialPointsDataFrame(wx.avg.jun.T[c("lon", "lat")], 
                                                  wx.avg.jun.T,
                                                  proj4string = CRS("+proj=longlat +datum=WGS84")), CRS(proj.USGS))

# Add a filter to exclude stations with less than 1 in of precipitation over a whole year
wx.ann.P <- wx %>%
  group_by(STATION, lon, lat) %>%
  summarize(sumprecip = sum(PRCP, na.rm=T)) 

bad.precip.stations = wx.ann.P %>% ungroup() %>% filter(sumprecip < 1) %>% dplyr::select(STATION)

wx.ann.P <- wx.ann.P %>% filter(sumprecip >= 1)

wx.prcp.proj <- spTransform(SpatialPointsDataFrame(wx.ann.P[c("lon", "lat")], 
                                                   wx.ann.P,
                                                   proj4string = CRS("+proj=longlat +datum=WGS84")), CRS(proj.USGS))

# Total snow
wx.ann.S <- wx %>%
  group_by(STATION, lon, lat) %>%
  summarize(sumsnow = sum(SNOW, na.rm=T)) 

# bad.precip.stations = wx.ann.P %>% ungroup() %>% filter(sumprecip < 1) %>% dplyr::select(STATION)

# wx.ann.P <- wx.ann.P %>% filter(sumprecip >= 1)

wx.snow.proj <- spTransform(SpatialPointsDataFrame(wx.ann.S[c("lon", "lat")], 
                                                   wx.ann.S,
                                                   proj4string = CRS("+proj=longlat +datum=WGS84")), CRS(proj.USGS))


# Check with plot
system('mkdir -p Figures')
pdf(file = paste0("Figures/WX_Maps_Bellevue_Point.pdf"), width = 10, height = 8)

plot(wx.prcp.proj, pch = '')
plot(rd_shp, col = 'lightgrey', add = T)
tempcol <- colorRampPalette(c("purple", "blue", "green", "yellow", "orange", "red"))
cuts = cut(wx.jan.proj$avgtempmin, 12)
points(wx.jan.proj$lon, wx.jan.proj$lat,
       col = tempcol(12)[cuts], pch = 16, cex = 2)
legend("bottom", pch = 16, col = tempcol(12),
       legend = levels(cuts),
       cex = 0.7, ncol = 4, pt.cex = 2)
title(main = "Jan 2018 average low temperatures \n King County, WA")

plot(wx.prcp.proj, pch = '')
plot(rd_shp, col = 'lightgrey', add = T)
tempcol <- colorRampPalette(c("purple", "blue", "green", "yellow", "orange", "red"))
cuts = cut(wx.jun.proj$avgtempmax, 12)
points(wx.jun.proj$lon, wx.jun.proj$lat,
       col = tempcol(12)[cuts], pch = 16, cex = 2)
legend("bottom", pch = 16, col = tempcol(12),
       legend = levels(cuts),
       cex = 0.7, ncol = 4, pt.cex = 2)
title(main = "June 2018 average high temperatures \n King County, WA")

plot(wx.prcp.proj, pch = '')
plot(rd_shp, col = 'lightgrey', add = T)
preccol <- colorRampPalette(c("white", "bisque", "green", "cornflowerblue", "blue", "purple"))
cuts = cut(wx.prcp.proj$sumprecip, 12)
points(wx.prcp.proj$lon, wx.prcp.proj$lat,
       bg = preccol(12)[cuts], pch = 21, cex = 2)
legend("bottom", pch = 21, pt.bg = preccol(12),
       legend = levels(cuts),
       cex = 0.7, ncol = 4, pt.cex = 2)
title(main = "Total precipitation \n King County, WA")

plot(wx.prcp.proj, pch = '')
plot(rd_shp, col = 'lightgrey', add = T)
preccol <- colorRampPalette(c("white", "bisque", "green", "cornflowerblue", "blue", "purple"))
cuts = cut(wx.snow.proj$sumsnow, 12)
points(wx.snow.proj$lon, wx.snow.proj$lat,
       bg = preccol(12)[cuts], pch = 21, cex = 2)
legend("bottom", pch = 21, pt.bg = preccol(12),
       legend = levels(cuts),
       cex = 0.7, ncol = 4, pt.cex = 2)
title(main = "Total snow \n King County, WA")

dev.off()

