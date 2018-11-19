# Hot spot of compiled special event in Bellevue, WA
# Based on work with Mobileye data 
# https://github.com/dflynn-volpe/Bus_Ped/blob/master/Route_12_kde2.R
# And single-event mapping with analysis/Hot_Spot/Hot_spot_1_Event.R


# Setup ----
# If you don't have these packages: install.packages(c("maps", "sp", "rgdal", "rgeos", "tidyverse")) 
# ggmap: want development version: devtools::install_github("dkahle/ggmap")
library(maps)
library(sp)
library(rgdal)
library(rgeos)
library(ggmap)
library(spatstat) # for ppp density estimation. devtools::install_github('spatstat/spatstat')
library(tidyverse)


codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary

setwd(localdir) 

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))
# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# load data ----

# Event state
state = "WA"

wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
wazemonthfiles = wazemonthfiles[-omit]

use.tz <- "US/Pacific"


# Compile events ----

loc = c(-122.156285, 47.592306) # Bellevue, WA


# Empty objects to store output, will rbind at the end of loop
waze.ll = waze.ll.proj = res.tab = vector()
  
for(i in 1:length(wazemonthfiles)){ # Start loop; i = 1
  
    load(file.path(wazedir, wazemonthfiles[i])) # load dataframe mb
  
  # Make both Waze and EDT spatial data frame, only picking out relevant columns. w and e are SPDF of Waze and EDT data, for this month
  w <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF
  
  w <- spTransform(w, CRS(proj))
  # Subset to area of interest only ---- 
  zoomll = data.frame(Lon = loc[1], Lat = loc[2])
  
  zoomll = SpatialPoints(zoomll, 
                         proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  zoomll <- spTransform(zoomll, CRS(proj))
  
  zoom_box = gBuffer(zoomll, width = 9654) # 6 mile circle Bellevue
  
  zoom_box.ll = spTransform(zoom_box, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
  
  # Step through Waze data to clip to special event buffer and day
  ws = gIntersects(zoom_box, w, byid = T)
  ws = w[ws[,1] == TRUE,] # Make ws as a spatial points data frame of Waze events, for points intersecting the buffer, in this month
  ws.ll = spTransform(ws, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
 
  # Get data frames for plotting. wp and ep are data.frames of ws and es, with projected coordinates
  wp <- data.frame(ws.ll@data, lat = coordinates(ws)[,2], lon = coordinates(ws)[,1])
  
  ll <- data.frame(lat = coordinates(ws.ll)[,2], lon = coordinates(ws.ll)[,1], alert_type = ws.ll$alert_type, time = ws.ll$time, roadclass = ws.ll$road_type)
  
  ll.proj <- data.frame(lat = coordinates(ws)[,2], lon = coordinates(ws)[,1], alert_type = ws$alert_type, time = ws$time, roadclass = ws$road_type)
  
  # Compile
  waze.ll = rbind(waze.ll, ll)
  waze.ll.proj = rbind(waze.ll.proj, ll.proj)
 
} # End special events day loop



# Save for plotting (will need to expand filename if doing multiple special events in a month) 
# w   Waze events for selected month and state, as spatial points data frame
# e   EDT crashes for selected month and state, SPDF
# ws  Geographic zoom of Waze events 
# es  Geographic zoom of EDT crahes
# zoom_box  Bounding area for the geographic zoom
# zoomll    Lat/long of special event location at center of bounding area

# *.ll versions indicate represented as lat long instead of projected in Albers Equal Area; this makes mapping with ggmap more convenient for now. *.proj is projected versions

fn = paste(state, "Bellevue_prep.RData", sep="_")

save(list = c("w", "ws", "zoom_box", "zoomll",
              "ws.ll", "ll.proj", "zoom_box.ll"),
     file = file.path(localdir, "WA", 
                      fn))

fn.export = paste(state, 
                "Bellevue_Prep_Export.RData", sep="_")

save(list = c("zoom_box", "zoomll",
              "waze.ll", "waze.ll.proj",
              "zoom_box.ll"),
     file = file.path(localdir, "WA", 
                      fn.export))


# Save to S3
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

zipname = paste0('Hot_Spot_Mapping_Bellevue_prep_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "WA", fn.export))
)

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


# Plotting ----

# Get a map for plotting.

if(length(grep(paste0(state, "_Bellevue_Basemaps"), dir(file.path(localdir, "WA")))) == 0){
  map_terrain_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'terrain', zoom = 14)
  map_toner_hybrid_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'toner-hybrid', zoom = 14)
  map_toner_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'toner', zoom = 14)
  save(list = c("map_terrain_14", "map_toner_hybrid_14", "map_toner_14"),
       file = file.path(localdir, "WA", paste0(state, "_Bellevue_Basemaps.RData")))
} else { load(file.path(localdir, "WA", paste0(state, "_Bellevue_Basemaps.RData"))) }


# ggmap(map_terrain_14)
# ggmap(map_toner_hybrid_14)
# ggmap(map_toner_14)


par.reset = par(no.readonly = T)

fignames = vector() # to store name of .PDF files, for zipping and export

# First, make a combined heat map for all event day, all non-event days, and the difference.

# Make maps for all 

waze.ll$Date = format(waze.ll$time, "%Y-%m-%d")
waze.ll$YM = format(waze.ll$time, "%Y-%m")


table(waze.ll %>% filter(alert_type =="ACCIDENT") %>% select(YM))

ggplot(waze.ll %>% filter(alert_type == "ACCIDENT" & YM != "2017-03" & YM != "2018-09")) +
  geom_histogram(aes(x = YM), stat="count") +
  ylab("Count of Waze accident reports") + xlab("Year-month") +
  ggtitle("Count of Waze accident reports by month within 6-mile buffer \n
          of Bellevue, WA city center")


  figname = file.path(localdir, 'Figures', paste(state, "Bellevue_Hot_Spot.pdf", sep="_"))
  
  pdf(file = figname, width = 10, height = 10)
  
  lx = waze.ll

  # 1. 
  # Waze mapped. 
  # Relies on kernel density estimation with MASS::kde2d
  # dd <- MASS::kde2d(x = ll$lon, y = ll$lat) # values are density per degree; would like convert this to 
  # dd.proj <- MASS::kde2d(x = ll.proj$lon, y = ll.proj$lat); contour(dd.proj)
  
  map1 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx, aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map1 + ggtitle(paste0("Waze event density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 2. Waze with points 
  map2 = map1 + 
    geom_point(data = lx, 
               aes(x = lon, y = lat, color = alert_type), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map2 + ggtitle(paste0("Waze event density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  # Waze Accident only versions
  # 1. 
  # Waze mapped. 

  map1 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx %>% filter(alert_type == "ACCIDENT"), aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map1 + ggtitle(paste0("Waze crash density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Crash density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 2. Waze with points 
  map2 = map1 + 
    geom_point(data = lx %>% filter(alert_type == "ACCIDENT"), 
               aes(x = lon, y = lat, color = alert_type), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map2 + ggtitle(paste0("Waze crash density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  dev.off()
  
  
  # Save output
  
  # save(list = c('dd','dd.e'), file = file.path(localdir, 'SpecialEvents', paste0("Special_Event_Hot_Spot_", specialeventtype, ".RData")))
  
  
# Save to export

zipname = paste0('Hot_Spot_Mapping_Bellevue_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             fignames)
)

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))





# Second, plot individual event / nonevent day pairs.


figname = file.path(localdir, 'Figures', paste0("Special_Event_Hot_Spot_", specialeventday,".pdf"))
pdf(figname, height = 10, width = 10)

for(specialeventday in se.use$Date) { # specialeventday = se.use$Date[1]
  
  # 1. 
  # Waze mapped. 
  # Relies on kernel density estimation with MASS::kde2d
  # dd <- MASS::kde2d(x = ll$lon, y = ll$lat) # values are density per degree; would like convert this to 
  # dd.proj <- MASS::kde2d(x = ll.proj$lon, y = ll.proj$lat); contour(dd.proj)
  
  map1 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
   # geom_density2d(data = ll, aes(x = lon, y = lat)) +
    stat_density2d(data = ll, aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map1 + ggtitle(paste0("Waze event density ", specialeventday))  +
    scale_fill_gradient(low = "blue", high = "red", 
                        guide_legend(title = "Event density")) +
    scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 2. Waze with points 
  map2 = map1 + 
              geom_point(data = ll, 
                         aes(x = lon, y = lat, color = alert_type), 
                         pch = "+", cex = 3, stroke = 2) 
    
  print(map2 + ggtitle(paste0("Waze event density ", specialeventday))  +
    scale_fill_gradient(low = "blue", high = "red", 
                        guide_legend(title = "Event density")) +
    scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                guide_legend(order = 2, title = "Waze alert type")))
  
  
  # 3. Same, with EDT points
  map3 = map2 + 
    geom_point(data = ll.e, 
               aes(x = lon, y = lat), 
               pch = "+", cex = 6, stroke = 2, color = "midnightblue") 
  
  print(map3 + ggtitle(paste0("Waze event density ", specialeventday))  +
    scale_fill_gradient(low = "blue", high = "red", 
                        guide_legend(title = "Event density")) +
    scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + 
    guides(color = guide_legend(order = 2, title = "Waze alert type")))
  
  
   dev.off()
  
  
  # Save output
  
  save(list = c('dd','dd.e'), file = file.path(localdir, 'SpecialEvents', paste0("Special_Event_Hot_Spot_", specialeventday, ".RData")))
  
  fignames = c(fignames, figname)

} # end day loop 


# Save to S3
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

# Get figs and files to export


zipname = paste0('Hot_Spot_Mapping_Multiple_', paste(range(eventdays), collapse="_to_"), "__", Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             fignames)
       )

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))



