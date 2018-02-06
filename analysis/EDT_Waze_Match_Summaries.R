# Explore Waze and EDT match tables and add indicator variables
# edited 1/31/2018

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----

library(dplyr)
library(tidyr)

setwd("~/")
if(length(grep("flynn", getwd())) > 0) {mappeddrive = "W:"} # change this to match the mapped network drive on your machine (Z: in original)
if(length(grep("EASdocs", getwd())) > 0) {mappeddrive = "S:"} #this line does not work for me
# mappeddrive = "S:" #for sudderth mapped drive, I have to click on the drive location in windows explorer to "connect" to the S drive before the data files will load

wazedir <- (file.path(mappeddrive,"SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"))
wazefigdir <- file.path(mappeddrive, "SDI Pilot Projects/Waze/Figures")
codeloc <- "~/GitHub/SDI_Waze" # Update as needed for the git repository on your local machine. (git for flynn, GitHub for sudderth)

setwd(wazedir)
getwd()

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

#Subset to Waze accidents
names(link.waze.edt)
dim(link.waze.edt)
table(link.waze.edt$type)
table(link.waze.edt$subtype)
table(link.waze.edt$subtype, link.waze.edt$type,useNA = "ifany")
table(link.waze.edt$type, link.waze.edt$match, useNA = "ifany")

#How many EDT reports match non-accident Waze events?
MatchTypeTable <- link.waze.edt %>%
  group_by(CrashCaseID) %>%
  summarize(
    nWazeAcc=table(type)[1],
    nWazeJAM=table(type)[2],
    nWazeRC=table(type)[3],
    nWazeWH=table(type)[4])

dim(MatchTypeTable) #9272

#Number of EDT reports with at least one Jam report and no accident reports
EDT_JamNoAcc <- filter(MatchTypeTable, nWazeJAM>1 & nWazeAcc<1)
nrow(EDT_JamNoAcc) #618

#Number of EDT reports with at least one RC report and no accident reports
EDT_RCNoAcc <- filter(MatchTypeTable, nWazeRC>1 & nWazeAcc<1)
nrow(EDT_RCNoAcc) #191

#Number of EDT reports with at least one WH report and no accident reports
EDT_WHNoAcc <- filter(MatchTypeTable, nWazeWH>1 & nWazeAcc<1)
nrow(EDT_WHNoAcc)

# <><><><><><><><><><><><>
# Add indicator variables to merged edt-waze file (IN PROGRESS)
# <><><><><><><><><><><><>

wazeAcc.edt <- filter(link.waze.edt, type=="ACCIDENT")

# Make sure EDT data is just for April
edt.april <- edt.april[edt.april$CrashDate_Local < "2017-05-01 00:00:00 EDT" & edt.april$CrashDate_Local > "2017-04-01 00:00:00 EDT",] 

# Add "M" code to the table to show matches if we merge in full datasets
match <- rep("M", nrow(link))
link <- mutate(link, match) #29,206
glimpse(link)

# Waze-waze:  Add "M" code to the table to show matches if we merge in full datasets
match <- rep("M", nrow(linkww))
linkww <- mutate(linkww, match) #29,206
glimpse(linkww)

# EDT-EDT:
match <- rep("M", nrow(linkee))
linkee <- mutate(linkee, match) #29,206
glimpse(linkee)





