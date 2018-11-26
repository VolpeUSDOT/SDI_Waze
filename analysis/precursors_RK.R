###########################
#
#
#  Raw WAZE event precursor exploration
#
#
####################################

codeloc <- "~/SDI_Waze"
library(sp)
library(tidyverse)
library(rgdal)
library(maps)

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use

localdir <- paste0(user, "/workingdata/") # full path for readOGR

wazedir <- "~/tempout" # has State_Year-mo.RData files. Grab from S3 if necessary
edtdir <- file.path(localdir, "EDT") # Unzip and rename with shortnames if necessary

setwd(localdir) #try mkdir ~/workingdata in terminal if this returns an error


# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Set parameters: states, yearmonths, time zone and projection
states = c("CT", "UT", "VA", "MD")

yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:4, width = 2, flag = "0"), sep="-")
)

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c(
                    "US/Eastern",
                    "US/Mountain",
                    "US/Eastern",
                    "US/Eastern"
                  ),
                  stringsAsFactors = F)




