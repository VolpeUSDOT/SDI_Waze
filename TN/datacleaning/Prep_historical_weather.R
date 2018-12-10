# Preparing historical weather from Global Historical Climate Network data, via NOAA.
# See SDI_Waze/wiki for details

# Setup ----
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

library(ggmap)
library(tidyverse)

user <- file.path( "/home", system("whoami", intern = TRUE)) # the user directory 
localdir <- file.path(user, "workingdata", "TN") # full path for readOGR

setwd(localdir)

# Load weather data ----

to_load <- dir(file.path(localdir, "Weather", "GHCN"))

d1 <- read.csv(file.path(localdir, "Weather", "GHCN", to_load[1]))
d2 <- read.csv(file.path(localdir, "Weather", "GHCN", to_load[2]))
d3 <- read.csv(file.path(localdir, "Weather", "GHCN", to_load[3]))

# vars <- c(colnames(d1), colnames(d2), colnames(d3))
# vars <- unique(vars)
# 
# all(vars %in% names(d1))
# all(vars %in% names(d2)); vars[!vars %in% names(d2)]
# all(vars %in% names(d3)); vars[!vars %in% names(d3)]
# After initial look, most stations don't have most of these variables. Simplify to core variables, per GHCN documentation:

core_vars = c("STATION", "NAME", "DATE", 
              "PRCP", "SNOW", "SNWD", "TMAX", "TMIN")

# vars = c("STATION", "NAME", "DATE", "AWND", "DAPR", "MDPR", "PGTM", "PRCP", "SNOW", "SNWD", "TAVG", "TMAX", "TMIN", "TOBS", "WSFG", "WT01", "WT02", "WT03", "WT04", "WT05", "WT06", "WT08", "WT10", "WT11")

dat <- rbind(d1[core_vars], d2[core_vars], d3[core_vars])
dat$DATE <- as.Date(as.character(dat$DATE))

dat <- dat[order(dat$DATE, dat$STATION),]

# To match with forecast, want the following by day
# date: yday, hour
# high: fahrenheit
# low: fahrenheit
# conditions
# qpf_allday: in
# snow_allday: in
# maxwind: mph
# avewind: mph


# Next steps: 
# Match to station lat longs, then 
# interpolate to state level using kriging or other methods, see 
# http://rspatial.org/analsis/rst/4-interpolation.html


