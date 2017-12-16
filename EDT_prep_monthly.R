# EDT prep
# Make monthly files from 1_CrashFact_edited.xlsx


# Setup ----

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

