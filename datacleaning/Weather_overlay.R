# Weather data overlay on grid aggregated data.
# Working with geoTIFFs from NEXRAD, from utility/geotiff_script.py

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(map)
library(sp)
library(rgdal)
library(rgeos)

# Set hexagon size
HEXSIZE = c("1", "4", "05")[1] # Change the value in the bracket to use 1, 4, or 0.5 sq mi hexagon grids


#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                       "WazeEDT Agg",HEXSIZE,"mile Rdata Input")
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- paste0("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/HexagonWazeEDT/",
                    "WazeEDT Agg",HEXSIZE,"mile Rdata Input")


source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles() function
setwd(wazedir)

# Read in weather data, starting now with April 1 2017
testmonth = "04"


weatherdir <- file.path(volpewazedir, "Data/Weather_Geotiffs")

testfile <- dir(weatherdir)[grep(paste0("2017-", testmonth), dir(weatherdir))][1]

# Attempts to read in weather data
wx <- readOGR(file.path(weatherdir, testfile))
wx <- raster(file.path(weatherdir, testfile))

# Get the hour and day and hour for this file
if(nchar(testfile) == 21){
   day <- strptime(substr(testfile, 1, 13), "%Y-%m-%d-%H") 
  } else {
   day <- strptime(substr(testfile, 1, 12), "%Y-%m-%d-%H") 
  }


# Read in aggregated data for this month
# WazeHexTimeList vs WazeTimeEdtHexAll? Larger files are TimeList, use those
wazedatamonth <- dir(wazemonthdir)[grep(paste0("WazeHexTimeList_", testmonth), dir(wazemonthdir))]

load(file.path(wazemonthdir, wazedatamonth))



