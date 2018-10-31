# Tennesee data initial formatting and exploration


# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile
source(file.path(codeloc, 'utility/Workstation_setup.R')) # Download necessary files from S3

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(lubridate)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # The user directory

setwd("~/workingdata/TN")

# Data explore

# Crash ----

# look at Crash/vwCollision.txt
# First time need to read from txt, afterwards can read from RData, ~ 2 seconds vs 5 minutes. Every column is read as character.
if(length(grep("TN_Crash.RData", dir('Crash')))==0){
  crash <- read.csv("Crash/vwCollision.txt", sep = '|')
  # format(object.size(crash), 'Mb')
  save(crash, file = "Crash/TN_Crash.RData")
} else {
  load('Crash/TN_Crash.RData')
}




# Special Events ----

# look at 'SpecialEvents/2018 Special Events.xlsx'; can use read_excel function in readxl package

# Weather ----

# script written to access forecasts in xml format. Consider what variables to use, and how to get historical weather.
