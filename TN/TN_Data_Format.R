# Tennesee data initial formatting and exploration


# Setup ----

# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to compile
source(file.path(codeloc, 'utility/Workstation_setup.R')) # Download necessary files from S3

library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(lubridate)
# Customized Functions
sumstats = function(x) { 
  null.k <- function(x) sum(is.na(x))
  unique.k <- function(x) {if (sum(is.na(x)) > 0) length(unique(x)) - 1
    else length(unique(x))}
  range.k <- function(x) max(x, na.rm=TRUE) - min(x, na.rm=TRUE)
  mean.k=function(x) {if (is.numeric(x)) round(mean(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  sd.k <- function(x) {if (is.numeric(x)) round(sd(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  min.k <- function(x) {if (is.numeric(x)) round(min(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  q05 <- function(x) quantile(x, probs=.05, na.rm=TRUE)
  q10 <- function(x) quantile(x, probs=.1, na.rm=TRUE)
  q25 <- function(x) quantile(x, probs=.25, na.rm=TRUE)
  q50 <- function(x) quantile(x, probs=.5, na.rm=TRUE)
  q75 <- function(x) quantile(x, probs=.75, na.rm=TRUE)
  q90 <- function(x) quantile(x, probs=.9, na.rm=TRUE)
  q95 <- function(x) quantile(x, probs=.95, na.rm=TRUE)
  max.k <- function(x) {if (is.numeric(x)) round(max(x, na.rm=TRUE), digits=2)
    else "N*N"} 
  
  sumtable <- cbind(as.matrix(colSums(!is.na(x))), sapply(x, null.k), sapply(x, unique.k), sapply(x, range.k), sapply(x, mean.k), sapply(x, sd.k),
                    sapply(x, min.k), sapply(x, q05), sapply(x, q10), sapply(x, q25), sapply(x, q50),
                    sapply(x, q75), sapply(x, q90), sapply(x, q95), sapply(x, max.k)) 
  
  sumtable <- as.data.frame(sumtable); names(sumtable) <- c('count', 'null', 'unique',
                                                            'range', 'mean', 'std', 'min', '5%', '10%', '25%', '50%', '75%', '90%',
                                                            '95%', 'max') 
  return(sumtable)
} 

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) # The user directory

setwd("~/workingdata/TN")

# Get Waze data ----

yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:8, width = 2, flag = "0"), sep="-"))

tn.ls = paste0("TN_", yearmonths, ".RData")

for(i in tn.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'TN'))))==0){
    
    system(paste("aws s3 cp",
                 file.path(teambucket, 'TN', i),
                 file.path('~', 'workingdata', 'TN', i)))
    
  }
}


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

table(crash$CrashTypeCde) # need information from TN State Petrol for more information
# 0      1      2      3      4      5      6     80     98 
# 21   3627  21418  54944 114219 574395  60574     96      1 

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
         , "IntersectionInd" # Whether it is an intersection             
         , "IntersectLocalIDTxt" # Intersection Location ID
         , "IntersectRoadNameTxt" # Intersection road name
         , "IntersectRoadNameSuffixTxt" # Name suffix
         , "IntersectRoadNbrTxt" # Intersection road/route number
         , "LatDecimalNmb" # Lat
         , "LongDecimalNmb" # Lon
         , "LightConditionCde" # light condition
         ) 
crash <- crash[,var]

# Data completeness
# crash[!complete.cases(crash),] # Error: C stack usage  8200812 is too close to the limit, the data is stil too large
# format(object.size(crash), "Mb") = 425 Mb

# Further reduce the data
var1 <- c("MstrRecNbrTxt", "CollisionDte", "LatDecimalNmb", "LongDecimalNmb") # only take ID, time, and location
crash1 <- crash[, var1]
crash1[!complete.cases(crash1),] # display rows with missing data, looks like most of them are just missing the geo-coordinates.

colSums(is.na(crash1)) # Number of missing data.
colSums(is.na(crash1))*100/nrow(crash1) # percent of missing data, ~29% missing Lat/Lon

round(colSums(is.na(crash))*100/nrow(crash), 2) # Large amounts of data are missing for some columns, such as BlockNbrTxt, IntersectionInd, IntersectLocalIDTxt, IntersectRoadNameTxt. If we plan to use these columns, need to understand why the missing data are not captured in is.na(). This might be due to blanks. 


# Looking at missing location info by year
crash$date = as.POSIXct(strptime(crash$CollisionDte, "%Y-%m-%d %H:%M:%S", tz = "US/Central"))
crash$year = as.numeric(format(crash$date, "%Y"))
# Fix data types
crash$MstrRecNbrTxt <- as.character(crash$MstrRecNbrTxt)

na.lat <- crash %>%
  filter(!is.na(year)) %>%
  group_by(fatal = NbrFatalitiesNmb > 0 , year) %>%
  summarize(naLat = sum(is.na(LatDecimalNmb) | LatDecimalNmb <= -99),
            N = n(),
            pct.na = 100*naLat/N)

ggplot(na.lat) +
  geom_point(aes(x = as.factor(year), y = pct.na, color = fatal)) +
  ylab("Percent missing Lat/Long") + xlab("Year") + 
  ggtitle("Summary of missing location by year and crash fatality for Tennessee")
ggsave("Output/Missing_lat_TN.jpeg")


var1 <- c("MstrRecNbrTxt", "date", "year", "LatDecimalNmb", "LongDecimalNmb") # only take ID, time, and location
crash1 <- crash[, var1]

sumstats(crash1[2:ncol(crash1)])


tn_crash <- crash1 %>% filter(LatDecimalNmb > 25 & LatDecimalNmb < 45 &
         LongDecimalNmb < -75 & LongDecimalNmb > -99)

sumstats(tn_crash[2:ncol(tn_crash)])


save("tn_crash", file = "Crash/TN_Crash_Simple_2008-2018.RData")

# Special Events ----

# look at 'SpecialEvents/2018 Special Events.xlsx'; can use read_excel function in readxl package



# Weather ----

# script written to access forecasts in xml format. Consider what variables to use, and how to get historical weather.
