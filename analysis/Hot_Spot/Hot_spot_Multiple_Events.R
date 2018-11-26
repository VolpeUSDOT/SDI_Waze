# Hot spot of compiled special event in Maryland
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
library(raster) # to work with density difference raster

codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary
edtdir <- file.path(localdir, "EDT") # Unzip and rename with shortnames if necessary

setwd(localdir) 

# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))
# Project to Albers equal area conic 102008. Check comparision wiht USGS version, WKID: 102039
proj <- showP4(showWKT("+init=epsg:102008"))

# load data ----

# Read special event data and convert it to spatial data format
SpecialEvents <- read.csv(file = file.path(localdir, "SpecialEvents", "SpecialEvents_MD_April17Sept_2018.csv"))

# Event state
state = "MD"

wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
wazemonthfiles = wazemonthfiles[-omit]

# get EDT data for this state 
e_i <- read.csv(file.path(edtdir, dir(edtdir)[grep(state, dir(edtdir))][1]),
                sep = "\t",
                na.strings = c("NA", "NULL")) 

# find rows with missing lat/long
# Discard rows with no lat long
cat(state, "EDT missing lat/long, FALSE TRUE: \n", summary(is.na(e_i$GPSLat)), "\n")
e_i <- e_i[!is.na(e_i$GPSLat) & !is.na(e_i$GPSLong),]
cat(state, "EDT lat/long marked 777, FALSE TRUE NA: \n", summary(e_i$GPSLat > 77 & e_i$GPSLong < -777), "\n")
e_i <- e_i[!e_i$GPSLat > 77 & !e_i$GPSLong < -777,]

# Make sure just this state represented
e_i <- e_i[e_i$CrashState == state.name[state.abb == state],]

use.tz <- "US/Eastern"

e_i$CrashDate_Local <- with(e_i,
                            strptime(
                              paste(substr(CrashDate, 1, 10),
                                    HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = use.tz)
)

# Compile events ----

# Begin loop to compile Waze and EDT events for this special event location, for event day and non-event days
# Will end up with four data frames, two each for Waze and EDT, for event day and non-event day
# Columns for each: lat, long, time, {alert_type}

# After compiling data frames, then carry out plotting.

specialeventloc = "SE2" # see levels(SpecialEvents$Location.ID). SE2 = Camden Yards

se.use = SpecialEvents %>% 
  filter(Location.ID == specialeventloc & EventType != "NoEvent") 

se.use$Date = as.Date(as.character(se.use$Date), "%m/%d/%Y") 

# Add comparison non-event days

non.event.df = vector()

for(i in 1:nrow(se.use)){ # i = 1
  non.event.date = se.use$Date[i] + 7

  # Test to make sure we get a day non in the special events data or otherise in non-event data
  if(length(non.event.df)==0){  
    while(non.event.date %in% se.use$Date) {
      non.event.date = non.event.date + 7
    }  
  } else {
    while(non.event.date %in% se.use$Date | non.event.date %in% non.event.df$Date) {
      non.event.date = non.event.date + 7
    }  
  }  
  
  non.event.row = se.use[i,]
  non.event.row$Date = non.event.date
  non.event.row$EventType = "NonEvent"
  non.event.row[c("StartTime","EndTime","Duration","Attendance")] = NA
  non.event.df= rbind(non.event.df, non.event.row)
}

# Compile special event and non events together
se.use = rbind(se.use, non.event.df)
se.use$Date.mo = format(se.use$Date, "%Y-%m")

# format time variables
se.use$DayofWeekN <- as.numeric(format(se.use$Date, format = "%u"))
se.use$Day <- as.numeric(format(se.use$Date, format = "%j"))
se.use$DayofWeek <- format(se.use$Date, format = "%A")

# Empty objects to store output, will rbind at the end of loop
waze.ll = waze.ll.proj = edt.ll = edt.ll.proj = vector()
  
for(i in 1:nrow(se.use)){ # Start event day loop; i = 1
  eventmo = se.use$Date.mo[i]
  
  specialeventday = se.use$Date[i]
  
  if(length(wazemonthfiles[grep(eventmo, wazemonthfiles)]) > 0){

      load(file.path(wazedir, wazemonthfiles[grep(eventmo, wazemonthfiles)])) # load dataframe mb
  
  # Make both Waze and EDT spatial data frame, only picking out relevant columns. w and e are SPDF of Waze and EDT data, for this month
  w <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF
  
  w <- spTransform(w, CRS(proj))
  # Subset to area of interest only ---- 
  zoomll = se.use %>% dplyr::select(Lon, Lat)
  zoomll = zoomll[!duplicated(zoomll),]
  
  zoomll = SpatialPointsDataFrame(zoomll[c("Lon", "Lat")], zoomll, 
                         proj4string = CRS("+proj=longlat +datum=WGS84"))
  
  zoomll <- spTransform(zoomll, CRS(proj))
  
  zoom_box = gBuffer(zoomll, width = 4827) # 3 mile circle around FedEx field
  
  zoom_box.ll = spTransform(zoom_box, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
  
  # Step through Waze data to clip to special event buffer and day
  ws = gIntersects(zoom_box, w, byid = T)
  ws = w[ws[,1] == TRUE,] # Make ws as a spatial points data frame of Waze events, for points intersecting the buffer, in this month
  ws.ll = spTransform(ws, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
 
  # Get data frames for plotting. wp and ep are data.frames of ws and es, with projected coordinates
  wp <- data.frame(ws.ll@data, lat = coordinates(ws)[,2], lon = coordinates(ws)[,1])
  
  # Now subset to just the day of interest
  selector = format(wp$time, "%Y-%m-%d") == specialeventday
  
  ll <- data.frame(lat = coordinates(ws.ll)[,2][selector], lon = coordinates(ws.ll)[,1][selector], alert_type = ws.ll$alert_type[selector], time = ws.ll$time[selector], roadclass = ws.ll$road_type[selector])
  
  ll.proj <- data.frame(lat = coordinates(ws)[,2][selector], lon = coordinates(ws)[,1][selector], alert_type = ws$alert_type[selector], time = ws$time[selector], roadclass = ws$road_type[selector])
  
  # Compile
  waze.ll = rbind(waze.ll, ll)
  waze.ll.proj = rbind(waze.ll.proj, ll.proj)
 
  # EDT ----
  # Subset to event month
  eb <- e_i[format(e_i$CrashDate_Local, "%Y-%m") == eventmo,]
  
  if(nrow(eb) > 0){
  e <- SpatialPointsDataFrame(eb[c("GPSLong", "GPSLat")], eb, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure EDT data is a SPDF
  e <- spTransform(e, CRS(proj))
  
  es = gIntersects(zoom_box, e, byid = T)
  es = e[es[,1] == TRUE,] # Make es as a spatial points data frame of EDT crashes, for points intersecting the buffer, in this month
  
  es.ll = spTransform(es, CRS("+proj=longlat +datum=WGS84")) # Back to lat long for mapping
  
  ep <- data.frame(es.ll@data, lat = coordinates(es)[,2], lon = coordinates(es)[,1])
  
  selector.e = format(ep$CrashDate_Local, "%Y-%m-%d") == specialeventday
  
  ll.e <- data.frame(lat = coordinates(es.ll)[,2][selector.e], lon = coordinates(es.ll)[,1][selector.e],
                     es.ll[selector.e,])
  
  ll.e.proj <- data.frame(lat = coordinates(es)[,2][selector.e], lon = coordinates(es)[,1][selector.e],
                          es.ll[selector.e,])
  
  # Compile
  edt.ll = rbind(edt.ll, ll.e)
  edt.ll.proj = rbind(edt.ll.proj, ll.e.proj)
  } # end if nrow(eb > 0)
  
  }

} # End special events day loop



# Save for plotting (will need to expand filename if doing multiple special events in a month) 
# w   Waze events for selected month and state, as spatial points data frame
# e   EDT crashes for selected month and state, SPDF
# ws  Geographic zoom of Waze events 
# es  Geographic zoom of EDT crahes
# zoom_box  Bounding area for the geographic zoom
# zoomll    Lat/long of special event location at center of bounding area

# *.ll versions indicate represented as lat long instead of projected in Albers Equal Area; this makes mapping with ggmap more convenient for now. *.proj is projected versions

fn = paste(state, min(se.use$Date), "to", max(se.use$Date),
            specialeventloc, "SpecialEvents.RData", sep="_")

save(list = c("w", "e", "ws", "es", "zoom_box", "zoomll",
              "ws.ll", "es.ll",
              "waze.ll", "waze.ll.proj", "edt.ll", 
              "ll.proj", "edt.ll.proj", "zoom_box.ll"),
     file = file.path(localdir, "SpecialEvents", 
                      fn))

fn.export = paste(state, 
                min(se.use$Date), "to", max(se.use$Date),
            specialeventloc, "SpecialEvent_Export.RData", sep="_")

save(list = c("zoom_box", "zoomll",
              "waze.ll", "waze.ll.proj",
              "edt.ll", "edt.ll.proj", "zoom_box.ll"),
     file = file.path(localdir, "SpecialEvents", 
                      fn.export))


# Save to S3
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

zipname = paste0('Hot_Spot_Mapping_Multiple_Export_', paste0(min(se.use$Date), "_to_", max(se.use$Date)), "__", Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "SpecialEvents", fn.export))
)

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


# Plotting ----

# Start from previous:
# load('/home/daniel/workingdata/SpecialEvents/MD_2017-02-26_to_2018-11-10_SE2_SpecialEvents.RData')
# load('/home/daniel/workingdata/SpecialEvents/MD_2017-02-26_to_2018-11-10_SE2_SpecialEvent_Export.RData')
# Get a map for plotting.

if(length(grep(paste0(state, "_", specialeventloc, "_Basemaps"), dir(file.path(localdir, "SpecialEvents")))) == 0){
  map_terrain_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'terrain', zoom = 14)
  map_toner_hybrid_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'toner-hybrid', zoom = 14)
  map_toner_14 <- get_stamenmap(bbox = as.vector(bbox(zoom_box.ll)), maptype = 'toner', zoom = 14)
  save(list = c("map_terrain_14", "map_toner_hybrid_14", "map_toner_14"),
       file = file.path(localdir, "SpecialEvents", paste0(state, "_", specialeventloc, "_Basemaps.RData")))
} else { load(file.path(localdir, "SpecialEvents", paste0(state, "_", specialeventloc, "_Basemaps.RData"))) }


# ggmap(map_terrain_14)
# ggmap(map_toner_hybrid_14)
# ggmap(map_toner_14)


par.reset = par(no.readonly = T)

fignames = vector() # to store name of .PDF files, for zipping and export

# First, make a combined heat map for all event day, all non-event days, and the difference.

# Make maps for all 

waze.ll$Date = format(waze.ll$time, "%Y-%m-%d")
edt.ll$Date = format(edt.ll$CrashDate_Local, "%Y-%m-%d")


se.use$EventType = as.factor(sub(" ", "_", as.character(se.use$EventType)))

for(specialeventtype in levels(se.use$EventType)) { # specialeventtype = 'Baseball'
  
  cat(specialeventtype, "\n\n")
  
  figname = file.path(localdir, 'Figures', paste(state, specialeventloc, specialeventtype, "Combined_Special_Event_Hot_Spot.pdf", sep="_"))
  
  pdf(file = figname, width = 10, height = 10)
  # Get the days for this event type
  use.dates = se.use %>% 
    filter(EventType == specialeventtype) %>%
    dplyr::select(Date)
  

  lx = waze.ll %>%
                 filter(Date %in% as.character(use.dates[,1]))
  
  lx.e = edt.ll[-which(names(edt.ll) == "CrashDate_Local")] %>% 
    filter(Date %in% as.character(use.dates[,1]))
  
  
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
               aes(x = lon, y = lat, color = alert_type, alpha = 0.5), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map2 + ggtitle(paste0("Waze event density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  # 3. Same, with EDT points
  map3 = map2 + 
    geom_point(data = lx.e, 
               aes(x = lon, y = lat), 
               pch = 16, cex = 3, stroke = 1, color = scales::alpha("midnightblue", 0.1)) 
  
  print(map3 + ggtitle(paste0("Waze event density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + 
          guides(color = guide_legend(order = 2, title = "Waze alert type")))
  

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
               aes(x = lon, y = lat, color = alert_type, alpha = 0.5), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map2 + ggtitle(paste0("Waze crash density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  # 3. Same, with EDT points
  map3 = map2 + 
    geom_point(data = lx.e, 
               aes(x = lon, y = lat), 
               pch = 16, cex = 3, stroke = 2, color = scales::alpha("midnightblue", 0.1)) 
  
  print(map3 + ggtitle(paste0("Waze crash density ", specialeventtype))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + 
          guides(color = guide_legend(order = 2, title = "Waze alert type")))
  
  
  # 4. Difference between EDT and Waze accident density
  # Set observation window
  pwin = owin(xrange = zoom_box.ll@bbox['x',],
              yrange = zoom_box.ll@bbox['y',])
  lx.acc = lx %>% filter(alert_type == "ACCIDENT")
  dp <- ppp(lx.acc$lon, lx.acc$lat, window = pwin)
  dd.w <- density(dp, edge = T)
  
  dp <- ppp(lx.e$lon, lx.e$lat, window = pwin)
  dd.e <- density(dp, edge = T)
  
  par(mfrow=c(2, 2), mar=rep(2,4))
  plot(dd.e, main="EDT Density"); plot(dd.w, main="Waze Density"); plot(dd.w-dd.e, main="Difference Waze-EDT")
  
  # Problem: spatstat images don't easily interact with ggmaps. And the RGoogleMap package no longer works (API rules changed). So need a workaround for plotting class `im` images from spatstat on ggmaps, or otherwise find a new mapping package
  
  # map5 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
  #   geom_raster()
  #   
  # print(map5 + ggtitle(paste0("Waze + EDT crash density ", specialeventtype))  +
  #         scale_fill_gradient(low = "blue", high = "red", 
  #                             guide_legend(title = "Crash density")) +
  #         scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  # 
  # 
  #im.map1 <- ggmap(map_toner_hybrid_14, extent = 'device')
  #im.map2 <- im.map1 + ggimage(dd.e)
  
  # Try to conver tto SpatialPixelsDataFrame
  # OR just export as geoTIFF
  
  dd.diff = dd.e-dd.w
  
  dr <- raster(dd.diff$v)
  crs(dr) <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
  extent(dr) = c(dd.diff$xrange, dd.diff$yrange)
  
  dwr <- raster(dd.w$v)
  crs(dwr) <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
  extent(dwr) = c(dd.w$xrange, dd.w$yrange)
  
  plot(dr, col = bpy.colors(255))
  plot(dwr, col = bpy.colors(255))
  
  plot(der, col = bpy.colors(255))
  
  plot(map_toner_hybrid_14)
  
  writeRaster(dr, filename = file.path(localdir, "SpecialEvents", paste("Diff", state, specialeventloc, specialeventtype, sep="_")),
              options = 'TFW=YES', overwrite=T)

  writeRaster(dwr, filename = file.path(localdir, "SpecialEvents", paste("Waze", state, specialeventloc, specialeventtype, sep="_")),
              options = 'TFW=YES', overwrite=T)
  
  dev.off()
  
  # Save output
  
  # save(list = c('dd','dd.e'), file = file.path(localdir, 'SpecialEvents', paste0("Special_Event_Hot_Spot_", specialeventtype, ".RData")))
  
  fignames = c(fignames, figname)
  
} # end day loop 

# Save to export

zipname = paste0('Hot_Spot_Mapping_Multiple_Figures_RasterLayers_', Sys.Date(), '.zip')

# Get raster layer names
rasters = dir(file.path(localdir, "SpecialEvents"))[grep(".gr", dir(file.path(localdir, "SpecialEvents")))]

system(paste('zip -j', file.path('~/workingdata', zipname),
             fignames,
             paste(file.path(localdir, "SpecialEvents", rasters)))
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



