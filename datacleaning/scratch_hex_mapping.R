# testing projections for hexagon mapping

# Setup ----
library(sp)
library(maps) # for mapping base layers
#library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles

wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"

#Sudderth drive
# wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
# wazedir <- "S:/SDI Pilot Projects/Waze/"
# volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
# outputdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

setwd(wazedir)

# Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, 
hex <- readOGR(file.path(volpewazedir, "spatial_layers/MD_hexagons_shapefiles"), layer = "MD_hexagons_1mi")

# read in county shapefile
co <- readOGR(file.path(wazedir, "Working Documents/Census Files"), layer = "cb_2015_us_county_500k")

# match coordinate reference system of hexagons to Urban Areas and counties
proj4string(hex) # Albers equal area, datum and ellipsoid same as census data projections
proj4string(co) # longlat

# maryland FIPS = 24
md.co <- co[co$STATEFP == 24,]

hex2 <- spTransform(hex, proj4string(md.co))


# try the other way

md.co.a <- spTransform(md.co, proj4string(hex))

plot(hex)
plot(md.co.a, add = T)
