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

# examine the crash data
names(crash)
dim(crash) # 829,301 rows * 80 columns

# Crash ID, it is unique? Yes.
length(unique(crash$MstrRecNbrTxt)) # 829,301, looks like this is a unique crash ID

table(crash$NbrUnitsNmb) # this column could include more than the number of vehicles
# 0      1      2      3      4      5      6      7      8      9     10     11     12     13     14     15     17     18     22     26     98 
# 3 219063 565583  37956   5359    969    243     73     18      9      8      4      1      2      1      1      1      1      1      1      1

# select the necessary columns for analysis (optional: save it as subset in the RData again?)
var <- c("MstrRecNbrTxt" # Unique crash ID
         , "CollisionDte" # Date of Crash
         , "CollisionTimeTxt" # Time of Crash
         , "NbrUnitsNmb" # Number of vehicles involved
         , "NbrFatalitiesNmb" # Fatals
         , "NbrInjuredNmb" # Injuries
         , "NbrNonInjuredNmb" # Non-Inguries
         , "NbrMotoristsNmb" # Number of drivers
         , "AlcoholInd" # whether alchohol is involved
         , "BlockNbrTxt" # Does not look like Census block
         , "CityCde" # City ID
         , "CountyStateCde" # County ID
         , "CrashTypeCde"  # What crash type do they have?
         ) 

# Special Events ----

# look at 'SpecialEvents/2018 Special Events.xlsx'; can use read_excel function in readxl package

# Weather ----

# script written to access forecasts in xml format. Consider what variables to use, and how to get historical weather.
