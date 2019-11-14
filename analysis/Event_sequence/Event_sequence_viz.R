# Event sequence visualization, from prepared data

# Setup


## Load libraries
library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(lubridate)
library(doParallel)
library(foreach)
library(sp)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

# aws.s3::get_bucket(output.loc) # forbidden 403, as aws.s3 relies on credentials, which we are not using. see: aws.signature::locate_credentials()
user <- paste0( "/home/", system("whoami", intern = TRUE)) #he user directory to use
output.loc <- paste0(user, "/tempout")

localdir <- paste0(user, "/workingdata/") # full path for dumping intermediate outputs
codeloc <- "~/SDI_Waze"

# read functions from utility files (just in case)
source(file.path(codeloc, 'utility/wazefunctions.R'))


zipname = dir(localdir)[grep("CT_Event_Sequence_Data_Prep_", dir(localdir))]

if(length(zipname)== 0){
  zipname = "CT_Event_Sequence_Data_Prep_2018-10-23.zip"
  system(paste("aws s3 cp",
               file.path(teambucket, 'SpecialEvents', zipname),
               file.path('~', 'workingdata', zipname)))
}

system(paste('unzip -o', file.path(localdir, zipname), '-d',
             file.path(localdir, "SpecialEvents"))) # Unzip to SpecialEvents directory

load(file.path(localdir, 'SpecialEvents', 'space.time.match.CT.Rdata'))
load(file.path(localdir, 'SpecialEvents', 'data.clusters.CT.Rdata'))
load(file.path(localdir, 'SpecialEvents', 'street.clusters.CT.Rdata'))
load(file.path(localdir, 'SpecialEvents', 'CT_Event_Sequence.RData'))

### 

