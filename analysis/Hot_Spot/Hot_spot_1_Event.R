# Hot spot of a special event in Maryland
# Based on work with Mobileye data 
# https://github.com/dflynn-volpe/Bus_Ped/blob/master/Route_12_kde2.R


# Setup ----
# If you don't have these packages: install.packages(c("maps", "sp","spatstat", "RgoogleMaps","ks","scales","tidyverse")) 
library(maps)
library(sp)
library(spatstat)
library(rgdal)
library(RgoogleMaps)
library(ks)
library(scales)
library(tidyverse)


# Event month and state
eventmo = "2017-09"
state = "MD"

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
SpecialEvents <- read.csv(file = file.path(localdir,"SpecialEvents", "SpecialEventsExpand_MD_AprilToSept_2017.csv"))


wazemonthfiles <- dir(wazedir)[grep(state, dir(wazedir))]
wazemonthfiles <- wazemonthfiles[grep("RData$", wazemonthfiles)]
# omit _Raw_
omit <- grep('_Raw_', wazemonthfiles)
wazemonthfiles = wazemonthfiles[-omit]

load(file.path(wazedir, wazemonthfiles[grep(eventmo, wazemonthfiles)])) # load dataframe mb

# get EDT data for this state 
e_i <- read.csv(file.path(edtdir, dir(edtdir)[grep(state, dir(edtdir))][1]),
                sep = "\t",
                na.strings = c("NA", "NULL")) 

# find rows with missing lat/long
# Discard rows with no lat long
cat(i, "EDT missing lat/long, FALSE TRUE: \n", summary(is.na(e_i$GPSLat)), "\n")
e_i <- e_i[!is.na(e_i$GPSLat) & !is.na(e_i$GPSLong),]
cat(i, "EDT lat/long marked 777, FALSE TRUE NA: \n", summary(e_i$GPSLat > 77 & e_i$GPSLong < -777), "\n")
e_i <- e_i[!e_i$GPSLat > 77 & !e_i$GPSLong < -777,]

# Make sure just this state represented
e_i <- e_i[e_i$CrashState == state.name[state.abb == state],]

use.tz <- "US/Eastern"

e_i$CrashDate_Local <- with(e_i,
                            strptime(
                              paste(substr(CrashDate, 1, 10),
                                    HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = use.tz)
)


# Subset to event month

eb <- e_i[format(e_i$CrashDate_Local, "%Y-%m") == eventmo,]


# Make both Waze and EDT spatial data frame, only picking out relevant columns
w <- SpatialPointsDataFrame(mb[c("lon", "lat")], mb, 
                            proj4string = CRS("+proj=longlat +datum=WGS84"))  # from monthbind of Waze data, make sure it is a SPDF

w <- spTransform(w, CRS(proj))

e <- SpatialPointsDataFrame(eb[c("GPSLong", "GPSLat")], eb, 
                              proj4string = CRS("+proj=longlat +datum=WGS84"))  #  make sure EDT data is a SPDF

e <-spTransform(e, CRS(proj))


# Process special events ----
# See SpecialEventsAnalysis.R
# format date
SpecialEvents$Date <- as.Date(SpecialEvents$Date, format = "%m/%d/%Y")

# format day of week
SpecialEvents$DayofWeekN <- as.numeric(format(SpecialEvents$Date, format = "%u"))


# Zoom in to event location


# Check with a plot:
plot(e, main = paste("EDT Waze Hot Spot overlay \n", state))
plot(w, add = T, col = "red")

# Subset to area of interest only ---- 

zoomll = SpecialEvents %>% filter(Location == 'Fedex Field') %>% select(Lon, Lat)
zoomll = zoomll[!duplicated(zoomll),]

zoomll = SpatialPointsDataFrame(zoomll[c("Lon", "Lat")], zoomll, 
                       proj4string = CRS("+proj=longlat +datum=WGS84"))

zoomll <-spTransform(zoomll, CRS(proj))

zoom_box = gBuffer(zoomll, width = 4827) # 3 mile circle around FedEx field

ws = gIntersects(zoom_box, w, byid = T)
ws = w[ws[,1] == TRUE,] # Make ws as a spatial points data frame of Waze events, for points intersecting the buffer

es = gIntersects(zoom_box, e, byid = T)
es = e[es[,1] == TRUE,] # Make es as a spatial points data frame of EDT crashes, for points intersecting the buffer

pdf(file.path("Figures", "Hot_Spot_data_prep.pdf"), width = 8, height = 8)
plot(zoom_box, main = 'Zooming to FedEx field, September 2017', sub='3 mile buffer')
plot(ws, add =T)
plot(es, add =T, col = 'red')
legend('topleft', pch = "+", col = c('black', 'red'), legend = c('Waze events', 'EDT crashes'))
dev.off()

# Get data frames for plotting 
wp <- data.frame(ws@data, lat = coordinates(ws)[,2], lon = coordinates(ws)[,1])
ep <- data.frame(es@data, lat = coordinates(es)[,2], lon = coordinates(es)[,1])

# Kernal density ----

# Settings
kern = "epanechnikov" #  "gaussian" # options to try: gaussian, epanechnikov, quartic and disc
bwidth = 0.5 # betwen 0 and 1

par.reset = par(no.readonly = T)

# Get a map for plotting. Save as object "mm", which we will use for convertting lat longs into plottable points 

mm <- plotmap(lat = wp$lat, lon = wp$lon,
              pch = 1,
              col = alpha("white", 0),
              maptype = "roadmap")

# Convert from lat long to plot-able XY for mapping.

# ll <- LatLon2XY.centered(mm, lat = coordinates(d)[,2], lon = coordinates(d)[,1])

eventdays = c("2017-09-10", "2017-09-17")

# loop over event / nonevent days..

for(specialeventday in eventdays) {
  selector = format(wp$time, "%Y-%m-%d") == specialeventday
  selector.e = format(ep$CrashDate_Local, "%Y-%m-%d") == specialeventday
  
 # ll <- LatLon2XY.centered(mm, lat = coordinates(ws)[,2][selector], lon = coordinates(ws)[,1][selector])
   ll <- data.frame(lat = coordinates(ws)[,2][selector], lon = coordinates(ws)[,1][selector])
   
   ll.e <- data.frame(lat = coordinates(es)[,2][selector.e], lon = coordinates(es)[,1][selector.e])

  # Skip Google maps for now
  
  # See ?kde. Normal kernel is default
  dl <- data.frame(ll$lon, ll$lat)
  dd <- kde(x = dl,
            H = Hscv(dl),
            adj.positive = 0.25)
  
  
  dl <- data.frame(ll.e$lon, ll.e$lat)
  dd.e <- kde(x = dl,
            H = Hscv(dl),
            adj.positive = 0.25)
  
  # Plotting ----
  # mm <- plotmap(lat = wp$lat, lon = wp$lon,
  #               pch = "+",
  #               cex = 0.8,
  #               col = alpha("grey20", 0.05),
  #               maptype = "roadmap")
  
  pdf(paste0("Special_Event_Hot_Spot_", specialeventday,".pdf"), height = 10, width = 10)
    
    # 1. Waze unmapped
    plot(dd, add = F, 
         drawpoints = F,
         display = "filled.contour",
         pch = "+",
         main = paste("Waze hot spot unmapped", specialeventday))
    
    # 2. Waze unmapped with points
    plot(dd, add = F, 
         drawpoints = T,
         display = "filled.contour",
         pch= "+",
         col.pt = scales::alpha("grey20", 0.3),
         main = paste("Special Event",  specialeventday)
    )
    
    # Get the contour levels
    levs <- contourLevels(dd, prob = seq(0, 1, by = 0.25))
    contcut <- cut(dd$cont, breaks = levs)
    # select the first instance of each contour level
    colordf <- data.frame(contcut, heat.colors(99))
    legcol <- colordf[!duplicated(colordf[,1]),]
    
    legend("topleft",
           title = "Waze Density (points per m2)",
           legend = legcol$contcut,
           fill = as.character(legcol$heat.colors.99))
    
    # Pixel values are estimated intensity values, expressed in "points per unit area".
    
    
    # 3. Same, with EDT points
    
    plot(dd, add = F, 
         drawpoints = T,
         display = "filled.contour",
         pch= "+",
         col.pt = scales::alpha("grey20", 0.3),
         main = specialeventday
    )
    
    points(dd.e$x,
         pch= "+",
         col = "blue", cex = 1.5)
    
    
    # Get the contour levels
    levs <- contourLevels(dd, prob = seq(0, 1, by = 0.25))
    contcut <- cut(dd$cont, breaks = levs)
    # select the first instance of each contour level
    colordf <- data.frame(contcut, heat.colors(99))
    legcol <- colordf[!duplicated(colordf[,1]),]
    
    legend("topleft",
           title = "Waze Density",
           legend = legcol$contcut,
           fill = as.character(legcol$heat.colors.99))
    
    legend("topright",
           title = "EDT",
           legend = 'EDT Crash',
           pch = "+", pt.cex = 1.5,
           col = "blue")
  
    # 4. EDT points and EDT hotspot
    
    plot(dd, add = F, 
         drawpoints = T,
         display = "filled.contour",
         pch= "+",
         col.pt = scales::alpha("grey20", 0.3),
         main = specialeventday
    )
    
    plot(dd.e, add = T, 
         drawpoints = F,
         display = "filled.contour")
    points(dd.e$x,
           pch= "+",
           col = "blue", cex = 1.5)
    
    
    # Get the contour levels
    levs <- contourLevels(dd, prob = seq(0, 1, by = 0.25))
    contcut <- cut(dd$cont, breaks = levs)
    # select the first instance of each contour level
    colordf <- data.frame(contcut, heat.colors(99))
    legcol <- colordf[!duplicated(colordf[,1]),]
    
    legend("topleft",
           title = "Waze Density",
           legend = legcol$contcut,
           fill = as.character(legcol$heat.colors.99))
    
    levs <- contourLevels(dd.e, prob = seq(0, 1, by = 0.25))
    contcut <- cut(dd.e$cont, breaks = levs)
    # select the first instance of each contour level
    colordf <- data.frame(contcut, heat.colors(99))
    legcol <- colordf[!duplicated(colordf[,1]),]
    
    legend("topright",
           title = "EDT Density",
           legend = legcol$contcut,
           fill = as.character(legcol$heat.colors.99))
    
    # 5. Difference of hot spots: TO DO
  
    # dd.diff = dd - dd.e    S
    
    
  dev.off()
  
  
  # Save output
  
  save(list = c('dd','dd.e'), file = file.path(localdir, 'SpecialEvents', paste0("Special_Event_Hot_Spot_", specialeventday, ".RData")))


} # end day loop 


# Save to S3

# Get figs and density files to export


zipname = paste0('Hot_Spot_Mapping_', paste(range(eventdays), collapse="_to_"), "__", Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(localdir, "SpecialEvents", fn),
             file.path('~/workingdata', paste0(state, "_All_Model_30.csv"))))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


# Difference between intensities ----
# To do
# 
# selector = format(wp$time, "%p") == "AM" #  AM/PM 
# ll <- LatLon2XY.centered(mm, lat = coordinates(d)[,2][selector], lon = coordinates(d)[,1][selector])
# dp <- ppp(ll$newX, ll$newY, window = pwin)
# dd.am <- density(dp, kernel = kern, adjust = bwidth, edge = T)
# 
# selector = format(wp$time, "%p") == "PM" # AM/PM 
# ll <- LatLon2XY.centered(mm, lat = coordinates(d)[,2][selector], lon = coordinates(d)[,1][selector])
# dp <- ppp(ll$newX, ll$newY, window = pwin)
# dd.pm <- density(dp, kernel = kern, adjust = bwidth, edge = T)
# 
# dd.diff <- dd.pm - dd.am
# 
# par(par.reset)
# plot(dd.diff, main = paste("PM - AM densities \n", kern, bwidth))
# dev.print(device = png, 
#           width = 600,
#           height = 600,
#           file = paste0(paste("Unmapped_PM-AM", kern, bwidth, sep = "_"), ".png"))


# titleloc <- XY2LatLon(mm, 2.221, 277.31) # use locator() to find where to put title
# TextOnStaticMap(mm, lat = titleloc[1], lon = titleloc[2], 
#                 paste(dataset, kern, bwidth), 
#                 add = T, pch = 2,
#                 font = 2)


# Using ppp version ----



dp <- ppp(ll$newX, ll$newY, window = pwin) # warning about duplicate points, double-check to make sure these are true duplicates

# See ?density.ppp
dd <- density(dp,
              kernel = kern,
              adjust = bwidth, # use this to change the bandwidth
              edge = T, # adjustment for edge of observation window, recommend T
              at = "pixels") # change to "points" to get the density exactly for each point

# Define the color map:
# make color map with increasing transparency at lower range
coln = 3*25 # make it divisible by 3 for following steps
col1 = rev(heat.colors(coln, alpha = 0.2))
col2 = rev(heat.colors(coln, alpha = 0.8))
col3 = rev(heat.colors(coln, alpha = 0.9))

col4 = c(col1[1:coln/3], col2[(coln/3+1):(2*coln/3)], col3[(1+2*coln/3):coln])

pc <- colourmap(col = col4, 
                range = range(dd))


# Plotting ----
mm <- plotmap(lat = dc$lat, lon = dc$lon,
              pch = "+",
              cex = 0.8,
              col = alpha("grey20", 0.05),
              maptype = "roadmap")
plot(dd, add = T, col = pc)

# Get the contour levels
levs <- quantile(dd, c(0.85, 0.95, 0.99, 1))
# select the color for of each contour level
legcol <- pc(levs)

# Pixel values are estimated intensity values, expressed in "points per unit area".

legend("topleft",
       title = "Density",
       legend = round(levs, 4),
       fill = legcol)


