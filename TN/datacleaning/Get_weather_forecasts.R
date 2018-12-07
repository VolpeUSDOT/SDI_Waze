# Get Weather forecasts for Tennessee, using Weatherunderground API


# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

library(ggmap)
library(httr) # for GET
library(tidyverse)
library(xml2) # for xml parsing in tidy way

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

xmls <- ls()[grep('\\.xml$', ls())]

save(list=xmls,
     file = file.path(localdir, "Weather", paste0("TN_Forecasts_", Sys.Date(), ".RData")))

forecasts <- dir(file.path(localdir, "Weather"))[grep("^TN_Forecasts_", dir(file.path(localdir, "Weather")))]

# Copy to S3
for(f in forecasts){
system(paste("aws s3 cp",
             file.path(localdir, "Weather", f),
             file.path(teambucket, "TN", "Weather", f)
       ))
}

# Parse XML ----

# Parse to data frame, apply to spatial grid, overlay on crash data for use in models
# Useful to look at elements of the XML file
# library(XML)
# dat <- xmlParse(get(xmls[1]))
# dat
# xmlToList(dat) 

# Within simpleforecast, pick out forecastdays, then each forecastday
# date: yday, hour
# high: fahrenheit
# low: fahrenheit
# conditions
# qpf_allday: in
# snow_allday: in
# maxwind: mph
# avewind: mph

dat <- vector()

# Loop over the xml files from this forecast and put into a data frame
for(i in 1:length(xmls)){
  check <- xmlToList(xmlParse(get(xmls[i])))[1]
  check <- grep("backend failure", check)
  if(length(check) > 0) { cat("XML query", xmls[i], "failed, skipping \n")
    
    } else { 
    
      d1 <- read_xml(get(xmls[i]))
    
      ll <- unlist(strsplit(innames[i], "/")); ll <- ll[length(ll)]
      ll <- unlist(strsplit(ll, ","))
      
      lat <- as.numeric(ll[1])
      lon <- as.numeric(sub("\\.xml", "", ll[2])) 
      yr <- d1 %>% xml_find_all("//date/year") %>% xml_text()
      ydays <- d1 %>% xml_find_all("//date/yday") %>% xml_text()
      th <- d1 %>% xml_find_all("//high/fahrenheit") %>% xml_double()
      tl <- d1 %>% xml_find_all("//low/fahrenheit") %>% xml_double()
      cond <- d1 %>% xml_find_all("//conditions") %>% xml_text()
      qpf <- d1 %>% xml_find_all("//qpf_allday/in") %>% xml_double() # accumulated precip
      snow <- d1 %>% xml_find_all("//snow_allday/in") %>% xml_double()
      maxwind <- d1 %>% xml_find_all("//maxwind/mph") %>% xml_double()
      avewind <- d1 %>% xml_find_all("//avewind/mph") %>% xml_double()
      
      dat <- rbind(dat, data.frame(lat, lon, yr, ydays, th, tl, cond, qpf, snow, maxwind, avewind))
    }
}

