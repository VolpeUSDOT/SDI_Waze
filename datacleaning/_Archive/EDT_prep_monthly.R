# EDT prep
# Make monthly files from 1_CrashFact_edited.xlsx
# Move to ARCHIVE 

# Setup ----
library(sp)

# read functions
codeloc <- "~/git/SDI_Waze"
source(file.path(codeloc, 'wazefunctions.R'))

if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} 
if(length(grep("sudderth", getwd())) > 0) {mappeddrive = "S:"} 

edtdir <- file.path(mappeddrive,"SDI Pilot Projects/Volpe/2017_11_03/waze/input/edt/2016_01_to_2017_09_MD_and_IN")
wazedir <- file.path(mappeddrive,"SDI Pilot Projects/Waze")

setwd(edtdir)

# Data import ----

edt <- read.csv("1_CrashFact_edited.csv")

edt.read <- edt # preseve full version while working 

# Keep only Maryland, only 2017

edt <- edt[edt$CrashState == "Maryland" & edt$StudyYear == "2017",]

# Format datetime. No crashdate_local, make it here
edt$CrashDate_Local <- with(edt,
                          strptime(
                            paste(substr(CrashDate, 1, 10),
                                  HourofDay, MinuteofDay), format = "%Y-%m-%d %H %M", tz = "America/New_York")
)

# Discard rows with no lat long
edt <- edt[!is.na(edt$GPSLat),]
# Loop through months, output csv and .Rdata. Use movefiles() to first write to a temporary directory, then move to SDI shared drive

outputdir_temp <- tempdir()
outputdir_final <- file.path(wazedir, "MASTER Data Files/EDT_month")

all.months <- format(edt$CrashDate_Local, "%m")
months <- sort(unique(all.months))

starttime <- Sys.time()

for(i in months){
  
  d <- edt[all.months == i,]
  
  
  d <- SpatialPointsDataFrame(d[c("GPSLong_New","GPSLat")], d)
  
  newobjname <- paste("edt", i, sep = "_")
  
  assign(newobjname, d)
  
  irda <- paste0("2017-", i, "_1_CrashFact_edited.RData.")
  icsv <- sub("RData", "csv", irda)
  
  save(list = newobjname, file = file.path(outputdir_temp, irda) )
  
  write.csv(d@data, file = file.path(outputdir_temp, icsv), row.names = F)
  
  timediff <- round(Sys.time()-starttime, 2)
  cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n\n")
  
}

filelist <- dir(outputdir_temp)[grep("[RData$|csv$]", dir(outputdir_temp))]
movefiles(filelist, outputdir_temp, outputdir_final)

  