# loading data 

setwd("W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped")
files <- dir()
for(i in files){ load(i) }

