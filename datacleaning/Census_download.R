# Downloading census files on SDC EC2 instance
# http://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_us_county_500k.zip

library(httr)

destination <- "~/workingdata/censusdownload"

system(paste("mkdir", destination))

request <- httr::GET("http://www2.census.gov/geo/tiger/GENZ2017/shp/cb_2017_us_county_500k.zip")
writeBin(httr::content(request, "raw"), file.path(destination, "cb_2017_us_county_500k.zip"))

unzip(file.path(destination, "cb_2017_us_county_500k.zip"))

