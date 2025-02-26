# Plot results of historical weather prep

# TODO: update this from Plot_weather_gridded.R; will need minimal changes to the data prep steps, but substantial changes to the plotting, since it will be by segment rather than grid ID.

library(dplyr)

mins <- wx.grd.day %>%
  group_by(ID) %>%
  summarize(minP = min(PRCP, na.rm=T),
            minTmin = min(TMIN, na.rm=T),
            minTmax = min(TMAX, na.rm=T),
            minS = min(SNOW, na.rm=T))

maxs <- wx.grd.day %>%
  group_by(ID) %>%
  summarize(maxP = max(PRCP, na.rm=T),
            maxTmin = max(TMIN, na.rm=T),
            maxTmax = max(TMAX, na.rm=T),
            maxS = max(SNOW, na.rm=T))

sumPS <- wx.grd.day %>%
  group_by(ID) %>%
  summarize(sumP = sum(PRCP, na.rm=T),
            sumS = sum(SNOW, na.rm=T))

wx.grd.day$mo = format(wx.grd.day$day, "%m")

wx.avg.jan.T <- wx.grd.day %>%
  group_by(ID) %>%
  filter(mo == "01") %>%
  summarize(avg.01.tempmax = mean(TMAX, na.rm=T),
            avg.01.tempmin = mean(TMIN, na.rm=T))

wx.avg.jun.T <- wx.grd.day %>%
  group_by(ID) %>%
  filter(mo == "06") %>%
  summarize(avg.06.tempmax = mean(TMAX, na.rm=T),
            avg.06.tempmin = mean(TMIN, na.rm=T))

pdf("Figures/WX_Segmenged_Bellevue.pdf", width = 8, height = 8)

  par(mfrow=c(2,2))
  hist(mins$minTmin, main = "Min TMIN by segment", col = "purple")
  hist(mins$minTmax, main = "Min TMAX by segment", col = "tomato")
  hist(mins$minP, main = "Min PRCP by segment", col = "cornflowerblue")
  hist(mins$minS, main = "Min SNOW by segment", col = "beige")
  
  hist(maxs$maxTmin, main = "Max TMIN by segment", col = "purple")
  hist(maxs$maxTmax, main = "Max TMAX by segment", col = "tomato")
  hist(maxs$maxP, main = "Max PRCP by segment", col = "cornflowerblue")
  hist(maxs$maxS, main = "Max SNOW by segment", col = "beige")

  par(mfrow=c(1,1))
  
  # Join to segments, make similar summaries as before
  wx.by.id <- left_join(mins, maxs)
  wx.by.id <- left_join(wx.by.id, sumPS)
  wx.by.id <- left_join(wx.by.id, wx.avg.jan.T)
  wx.by.id <- left_join(wx.by.id, wx.avg.jun.T)
  
  wx.by.id$ID <- as.character(wx.by.id$ID)
  
  plot_rd <- rd_shp
  plot_rd@data <- left_join(plot_rd@data, wx.by.id, by = c("OBJECTID"="ID"))
  
  plot_rd@data[plot_rd@data==-Inf] = NA
  
  # Make maxTmax, maxS, and sumP maps
  tempcol <- colorRampPalette(c("purple", "blue", "green", "yellow", "orange", "red"))
  cuts = cut(plot_rd@data$maxTmax, 10)
  plot(plot_rd, col = tempcol(12)[cuts])
  legend("left", pch = 15, col = tempcol(12),
         legend = levels(cuts),
         cex = 0.7, ncol = 1, pt.cex = 2)
  title(main = "Max temperatures over study period")
  
  cuts = cut(plot_rd@data$avg.01.tempmin, 10)
  plot(plot_rd, col = tempcol(12)[cuts])
  legend("left", pch = 15, col = tempcol(12),
         legend = levels(cuts),
         cex = 0.7, ncol = 1, pt.cex = 2)
  title(main = "Average January low temperatures")
  
  cuts = cut(plot_rd@data$avg.06.tempmax, 10)
  plot(plot_rd, col = tempcol(12)[cuts])
  legend("left", pch = 15, col = tempcol(12),
         legend = levels(cuts),
         cex = 0.7, ncol = 1, pt.cex = 2)
  title(main = "Average June high temperatures")
  
  preccol <- colorRampPalette(c("white", "bisque", "green", "cornflowerblue", "blue", "purple"))
  cuts = cut(plot_rd@data$sumP, 10)
  plot(plot_rd, col = preccol(12)[cuts])
  legend("left", pch = 15, col = preccol(12),
         legend = levels(cuts),
         cex = 0.7, ncol = 1, pt.cex = 2)
  title(main = "Sum of precipitation across the study period")
  
  
dev.off()


