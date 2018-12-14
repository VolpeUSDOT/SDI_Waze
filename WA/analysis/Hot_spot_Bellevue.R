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

wazedir <- file.path(localdir, "WA", "Waze") # has State_Year-mo.RData files. Grab from S3 if necessary

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

#commented the omit step out - removes all files
#omit <- grep('_Raw_', wazemonthfiles)
#wazemonthfiles = wazemonthfiles[-omit]
wazemonthfiles = wazemonthfiles[nchar(wazemonthfiles)==16]

use.tz <- "US/Pacific"


# Compile events ----

loc = c(-122.155, 47.59) # Bellevue, WA


# Empty objects to store output, will rbind at the end of loop
waze.ll = waze.ll.proj = w.all = w.all.proj = vector()
  
for(i in 1:length(wazemonthfiles)){ # Start loop; i = 1
    cat(wazemonthfiles[i], "\n")
  
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
  
  zoom_box = gBuffer(zoomll, width = 9654) # 6 mile circle around Bellevue
  
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
  w.all <- rbind(w.all, data.frame(ws.ll@coords, ws.ll@data))
  w.all.proj <- rbind(w.all.proj, data.frame(ws@coords, ws@data))
  
  waze.ll = rbind(waze.ll, ll)
  waze.ll.proj = rbind(waze.ll.proj, ll.proj)
 
} # End compile Waze event loop


# Save for plotting (will need to expand filename if doing multiple special events in a month) 
# w.all     Waze events for selected area, as spatial points data frame, with all variables
# waze.ll   Waze events for selected area, as spatial points data frame, with only alert type, road class, and lat long
# zoom_box  Bounding area for the geographic zoom
# zoomll    Lat/long of special event location at center of bounding area

# *.ll versions indicate represented as lat long instead of projected in Albers Equal Area; this makes mapping with ggmap more convenient for now. *.proj is projected versions

fn = paste(state, "Bellevue_prep.RData", sep="_")

save(list = c("zoom_box", "zoomll",
              "w.all", "w.all.proj",
              "waze.ll", "waze.ll.proj",
              "zoom_box.ll"),
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


table(waze.ll %>% filter(alert_type =="ACCIDENT" & roadclass != 1) %>% select(YM))

ggplot(waze.ll %>% filter(alert_type == "ACCIDENT" & YM != "2017-03")) +
  geom_histogram(aes(x = YM), stat="count") +
  ylab("Count of Waze accident reports") + xlab("Year-month") +
  ggtitle("Count of Waze accident reports by month within 6-mile buffer \n
          of Bellevue, WA city center")

# Omitting Highway 
nametable <- sort(table(as.factor(w.all$street)), decreasing = T)
nametable[1:100]

# Added additional exits and toll lanes - update to use Grep to find "Exit" and variations of highway names to get "to" roads?
to_omit = c("I-405", "I-90", "SR-520", "I-5", 
            "to I-405 N / Everett","to I-405 S / Renton",
            "to I-405 / Everett / Renton",
            "I-405 N Exp Toll Ln", "I-405 S Exp Toll Ln", 
            "to I-405 S", "to I-405 N",
            "to I-90 W / Seattle", "Exit 10: I-405 N / Bellevue / Everett",
            "Exit 10: I-405 S / Renton", "Exit 10A: I-405 / Everett / Renton",
            "Exit 10: Coal Creek Pkwy / Factoria Blvd SE",
            "Exit 10B: Factoria Blvd",
            "Exit 11: I-90 E / Spokane", "Exit 11: I-90 W / Seattle",
            "Exit 11B: 148th Ave SE",
            "Exit 9: Bellevue Way",
            "to I-90 E / Spokane",
            "to SR-520 E / Redmond", "to SR-520 W / Toll Bridge / Seattle",
            "Exit 13A: NE 4th St",
            "Exit 13: W Lake Sammamish / Lakemont Blvd SE / SE Newport Way",
            "to SR-202 / Redmond Way",
            "to SR-520 W / Seattle",
            "Exit 14: SR-520 / Seattle / Redmond",
            "to I-405 N / to I-90", "to SR-520 E", "to I-90 E", "to I-90 W",
            "Exit 12: SE 8th St",
            "Exit 11A: 150th Ave SE / Eastgate Way",
            "to W Lk Sammamish Pkwy NE",
            "Exit 14: SR-520 E / Redmond")

omits = vector()
for(i in to_omit){
  omits = c(omits, grep(paste0("^", i), w.all$street))
}
length(omits)
w.nohwy = w.all[is.na(match(1:nrow(w.all), omits)),]

nametable.nohwy <- sort(table(as.factor(w.nohwy$street)), decreasing = T)
nametable.nohwy[1:100]


# also omit by road_type = 3
table(w.nohwy$road_type)
table(w.all$road_type)

nametable <- sort(table(as.factor(w.nohwy$street)), decreasing = T)
nametable[1:25]

nametable.rt3 <- sort(table(as.factor(w.nohwy[w.nohwy$road_type == 3, "street"])), decreasing = T)
nametable.rt3

figname = file.path(localdir, 'Figures', paste(state, "Bellevue_Hot_Spot.pdf", sep="_"))
  
pdf(file = figname, width = 10, height = 10)

  lx = w.all %>% select(lon, lat, alert_type, sub_type, road_type, time) 

  lx.nohwy = w.nohwy %>% select(lon, lat, alert_type, sub_type, road_type, time) 

  # 1. 
  # Waze mapped. 
  # Relies on kernel density estimation with MASS::kde2d
  # dd <- MASS::kde2d(x = ll$lon, y = ll$lat) # values are density per degree; would like convert this to 
  # dd.proj <- MASS::kde2d(x = ll.proj$lon, y = ll.proj$lat); contour(dd.proj)
  
  map1 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx, aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map1 + ggtitle(paste0("Waze event density"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 2. Omitting highway

  map.nohwy <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx.nohwy, aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map.nohwy + ggtitle(paste0("Waze event density (omitting highways)"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # Waze Accident only versions
  # 1. 
  # Waze mapped. 

  map1 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx %>% filter(alert_type == "ACCIDENT"), aes(x = lon, y = lat,
                                  fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map1 + ggtitle(paste0("Waze crash density"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Crash density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 2. Waze with points 
  map2 = map1 + 
    geom_point(data = lx %>% filter(alert_type == "ACCIDENT"), 
               aes(x = lon, y = lat, color = alert_type), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map2 + ggtitle(paste0("Waze crash density"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  # Accident only, no highway
  
  # 3.
  # Waze mapped. 
  
  map3 <- ggmap(map_toner_hybrid_14, extent = 'device') + 
    stat_density2d(data = lx.nohwy %>% filter(alert_type == "ACCIDENT"), aes(x = lon, y = lat,
                                                                       fill = ..level.., alpha = ..level..),
                   size = 0.01, bins = 8, geom = 'polygon')
  
  print(map3 + ggtitle(paste0("Waze crash density (omitting highways)"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Crash density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE) )
  
  # 4. Waze with points 
  map4 = map3 + 
    geom_point(data = lx.nohwy %>% filter(alert_type == "ACCIDENT"), 
               aes(x = lon, y = lat, color = scales::alpha("midnightblue", 0.5)),#alert_type), 
               pch = "+", cex = 3, stroke = 2) 
  
  print(map4 + ggtitle(paste0("Waze crash density (omitting highways)"))  +
          scale_fill_gradient(low = "blue", high = "red", 
                              guide_legend(title = "Event density")) +
          scale_alpha(range = c(0.1, 0.8), guide = FALSE)  + guides(color = 
                                                                      guide_legend(order = 2, title = "Waze alert type")))
  
  
  dev.off()
  
  
# Save to export

zipname = paste0('Hot_Spot_Mapping_Bellevue_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             figname)
)

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))

