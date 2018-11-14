

# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

# Install libraries 
library(dplyr)
library(RPostgres)
library(getPass)
library(lubridate)
library(data.table)
library(sp)
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles

# turn off scienctific notation 
options(scipen = 999)


# read functions
source(file.path(codeloc, 'utility/wazefunctions.R'))


# Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))


# Set up workstation to get hexagon shapefiles 
source(file.path(codeloc, 'utility/Workstation_setup.R'))


### Waze Alert Query parameters

# Query parameters
# states with available EDT data for model testing
states = c("CT", "MD", "UT", "VA")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Eastern",
                         "US/Mountain",
                         "US/Eastern"),
                  stringsAsFactors = F)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling by State ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:8, width = 2, flag = "0"), sep="-")
)
yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

starttime <- Sys.time()

# For each state, grab already processed months of data with duration in there

for(state in states){ # state = 'UT'
  
  # Check S3 contents for this state
  # system(paste("aws s3 ls",
  #              paste0(file.path(teambucket, state), "/")))
                
  # Get these months data from S3 and compile
  getmonths <- paste0(state, "_", yearmonths, ".RData")
  
  alert_results <- vector()
  for(i in getmonths){
    if(length(grep(i, dir(file.path('~', 'workingdata', state))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, state, i),
                   file.path('~', 'workingdata', state, i)))
    }
    load(file.path("~", "workingdata", state, i))
    alert_results <- rbind(alert_results, mb)
    
  }
  


  # calculate estimated duration of the event in minutes 
  # take difference of time and last.pull.time; correct pull durations for 2017 and 2018 have already been done

  timediff <- alert_results$last.pull.time - alert_results$time
  stopifnot(attr(timediff, "units")=="secs")
    
  alert_results$event_duration_minutes <- as.numeric(timediff/60)
  
  # Select columns to export for Tableau
  cols = c("lon", "lat", "city", "alert_type", "sub_type", "street", "road_type", "time", "last.pull.time", "event_duration_minutes")
  
  alert_results = alert_results %>% 
    filter(alert_type != "ROAD_CLOSED" & sub_type != "HAZARD_ON_ROAD_CONSTRUCTION") %>%
    select(cols)
  
  # Looking at historgrams, limit to events lasting less than 3 hrs
  ggplot(alert_results %>% filter(event_duration_minutes < 60*3)) + 
    geom_histogram(aes(x=event_duration_minutes)) + 
    facet_wrap(~alert_type) + ggtitle(paste(state, "frequency of duration of events < 3 hrs"))
  ggsave(filename = paste0(file.path("~", "workingdata", "Figures/"), state, "_Event_Duration_Hist.jpg"))
  
  # Save to local, will zip and put in export after
  write.csv(alert_results, file = paste0(file.path(localdir, state, "/"), state, "_Event_Duration_to_", yearmonths[length(yearmonths)], ".csv"))
}



# Zip and export

zipfiles = vector()
for(state in states){
  zipfiles = c(zipfiles, paste0(file.path(localdir, state, "/"), state, "_Event_Duration_to_", yearmonths[length(yearmonths)], ".csv"))
}

zipname = paste0("Event_Durations_CT_MD_VA_UT_", Sys.Date(), ".zip")

for(z in zipfiles){
  system(paste('zip', file.path('~/workingdata', zipname), z))
}
  
system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))


