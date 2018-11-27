# Get Weather forecasts for Tennessee, using Weatherunderground API


# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))

library(ggmap)
library(httr)
library(tidyverse)

user <- file.path( "/home", system("whoami", intern = TRUE)) # the user directory 
localdir <- file.path(user, "workingdata", "TN") # full path for readOGR

setwd(localdir)

# Get weather data ----

# Provided by TN: WeatherUnderground_171228.txt. This is a small script to be run in a Windows enviroment, to get XML-format files for 11 locations in TN. Here we'll read in the file as is, just to extract the API addresses
# WUndground *no longer is providing free API keys*, so we will see if the API key in these addresses will work. Free version works through end of 2018.

wu <- scan(file = "Weather/WeatherUnderground_171228.txt",
           what = 'character')


xmlnames <- wu[grep(".xml$", wu)]

outnames <- xmlnames[nchar(xmlnames) < 15]
innames <- xmlnames[nchar(xmlnames) > 15]

for(i in 1:length(innames)){
  cat('Fetching', outnames[i], "\n")
  assign(outnames[i], GET(innames[i]))
  
}

# Save forecasts ----

xmls <- ls()[grep('xml', ls())]

save(list=xmls,
     file = file.path(localdir, "Weather", paste0("TN_Forecasts_", Sys.Date(), ".RData")))


# TO DO ----
# Parse to data frame, apply to spatial grid, overlay on crash data for use in models
