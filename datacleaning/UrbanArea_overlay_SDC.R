# Assigns urban area classifications and hexagonal grid IDs to Waze and EDT events
# Merges the two files into a matched data frame and saves
# Loops over all months of available data where both EDT and Waze files exist

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
# Check for package installations
codeloc <- "~/SDI_Waze"
source(file.path(codeloc, 'utility/get_packages.R'))


library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles

output.loc <- "~/tempout"
localdir <- "/home/daniel/workingdata/" # full path for readOGR

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

source(file.path(codeloc, "utility/wazefunctions.R")) 

states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"),
                  stringsAsFactors = F)


setwd(localdir)

TEST = F      # Change to F to run for all available months, T for only a subset of months
CHECKPLOT = F # Make plots for each stage, to make sure of spatial overlay

# Read in spatial data ----

# Read in urban/rural layer from Census
# From: https://www.census.gov/geo/maps-data/data/cbf/cbf_ua.html
# https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshprd13/TGRSHPrd13_TechDoc_A.pdf
# Get from S3 if necessary

ua <- readOGR(file.path(localdir, "census"), layer = "cb_2016_us_ua10_500k")

# Read in county shapefile
co <- readOGR(file.path(localdir, "census"), layer = "cb_2017_us_county_500k")

# Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, national
hex <- readOGR(file.path(localdir, "Hex"), layer = "hexagons_1mi_lower48_neighbors")

# !!! Error: C stack usage  34724785 is too close to the limit
# https://stackoverflow.com/questions/14719349/error-c-stack-usage-is-too-close-to-the-limit
# default 8192, 8 Mb
# change to 16 Mb:
# system("ulimit -s 16384") # and then restart
# R --slave -e 'Cstack_info()["size"]'

# match coordinate reference system of hexagons to Urban Areas and counties
# proj4string(hex)
# proj4string(co)

# maryland FIPS = 24
md.co <- co[co$STATEFP == 24,]

hex2 <- spTransform(hex, proj4string(md.co))
# grid column names
gridcols <- names(hex2)[grep("^GRID", names(hex2))]

# EDT: See CrashFact in W:\SDI Pilot Projects\Volpe\2017_11_03\waze\input\edt\2016_01_to_2017_09_MD_and_IN.

# files
edt.monthly <- dir(file.path(wazedir, "MASTER Data Files/EDT_month/"))
waze.monthly <- dir(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/"))

avail.edt.months <- substr(edt.monthly[grep("CrashFact_edited.RData", edt.monthly)],
                           start = 6, stop = 7)
avail.waze.months <- unique(substr(waze.monthly[grep("MD_buffered__", waze.monthly)],
                                   start = 19, stop = 20))

shared.avail.months = c(avail.edt.months, avail.waze.months)[duplicated(c(avail.edt.months, avail.waze.months))]

if(TEST) shared.avail.months = c("04") # Change TEST to F to run for all available months

# Start loop over months ----

# Save files first to a local temporary directory, then move to the shared drive and delete local copies when complete, using movefiles() from utility/wazefunctions.R.
temp.outputdir = tempdir() 


for(i in shared.avail.months){
  
  load(file.path(wazedir, paste0("MASTER Data Files/EDT_month/2017-", i, "_1_CrashFact_edited.RData")))
  # find the edt file for this month and assign to temporary working file name. Remove the month-named data frame
  edt.working <- get(ls()[grep("edt_", ls())]); rm(list=ls()[grep("edt_", ls())])
  
  # Waze: Comes from aggregated monthly Waze events, clipped to a 0.5 mile buffer around MD, for each month 2017. All the waze files share the same object name, d
  load(file.path(wazedir, paste0("MASTER Data Files/Waze Aggregated/month_MD_clipped/MD_buffered__2017-", i, ".RData")))
  
  link <- read.csv(file.path(wazedir, paste0("MASTER Data Files/Waze Aggregated/month_MD_clipped/EDT_Waze_link_2017-", i, "_MD.csv")))
  
  # Make sure EDT data is just for the month of interest
  edt.working <- edt.working[!is.na(edt.working$CrashDate_Local),]
  edt.working <- edt.working[edt.working$CrashDate_Local < 
                               paste0("2017-", formatC(as.numeric(i)+1, width = 2, flag = "0"), "-01 00:00:00 EDT") & 
                               edt.working$CrashDate_Local > paste0("2017-", i,"-01 00:00:00 EDT"),] 
  
  
  # Add "M" code to the table to show matches if we merge in full datasets
  match <- rep("M", nrow(link))
  link <- mutate(link, match) 
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Overlay points in polygons
  # Use over() from sp to join these to the census polygon. Points in Polygons, pip
  
  proj4string(d) <- proj4string(edt.working) <- proj4string(ua) 
  
  # First clip EDT to state 
  EDT.co <- over(edt.working, md.co["COUNTYFP"])
  edt.working <- edt.working[!is.na(EDT.co$COUNTYFP),] # drop 33 points in April 2017 MD of 9308
  # <><><><><><><><><><><><><><><><><><><><>
  # Urban area overlay ----
  
  waze_ua_pip <- over(d, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in d. 
  edt_ua_pip <- over(edt.working, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in edt.working. 
  
  d@data <- data.frame(d@data, waze_ua_pip)
  names(d@data)[(length(d@data)-1):length(d@data)] <- c("Waze_UA_Name", "Waze_UA_Type")
  
  edt.working@data <- data.frame(edt.working@data, edt_ua_pip)
  names(edt.working@data)[(length(edt.working@data)-1):length(edt.working@data)] <- c("EDT_UA_Name", "EDT_UA_Type")
  
  # <><><><><><><><><><><><><><><><><><><><>
  # Hexagon overlay
  
  waze_hex_pip <- over(d, hex2[,gridcols]) # Match hexagon names to each row in d. 
  edt_hex_pip <- over(edt.working, hex2[,gridcols]) # Match hexagon names to each row in edt.working. 
  # add these columns to the data frame
  d@data <- data.frame(d@data, waze_hex_pip)
  
  edt.working@data <- data.frame(edt.working@data, edt_hex_pip)
  
  # Check:
  if(CHECKPLOT){  
    plot(d)
    points(edt.working, col = "red")
    plot(md.co, add = T, col = alpha("grey80", 0.1))
    dev.print(jpeg, file = file.path(volpewazedir, paste0("Figures/Checking_EDT_Waze_UAoverlay_", i, ".jpg")), width = 500, height = 500)
  }
  # <><><><><><><><><><><><><><><><><><><><>
  # Merge and save EDT-Waze
  # <><><><><><><><><><><><><><><><><><><><>
  
  # Save Waze data as dataframe
  waze.df <- d@data 
  # names(waze.df)
  
  # Save EDT data as dataframe
  edt.df <- edt.working@data 
  # names(edt.df)
  
  # before carrying out the join, rename the EDT grid cell columns 
  names(edt.df)[grep("^GRID", names(edt.df))] <- paste(names(edt.df)[grep("^GRID", names(edt.df))], "edt", sep = ".")
  
  # Join Waze data to link table (full join)
  link.waze <- full_join(link, waze.df, by=c("id.incidents"="uuid"))
  
  # Add W code to match column to indicate only Waze data
  link.waze$match <- ifelse(is.na(link.waze$match), 'W', link.waze$match)
  
  # Join EDT data to Waze-link table (full join)
  # *** Potential improvement for data storage: at this step, make a selection of only those columns which will be useful from EDT (do not include all 0 columns or columns not useful for the model) ***
  
  edt.df$CrashDate_Local <- as.character(edt.df$CrashDate_Local) # will need to convert back to POSIX
  link.waze.edt <- full_join(link.waze, edt.df, by = c("id.accident"="ID")) 
  
  #Add E code to match column to indicate only EDT data
  link.waze.edt$match <- ifelse(is.na(link.waze.edt$match), 'E', link.waze.edt$match)
  # table(link.waze.edt$match)
  
  # Convert CrashDate_Local back to POSIX
  link.waze.edt$CrashDate_Local <- strptime(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
  
  # rename ID variables for compatibility with existing code
  names(link.waze.edt)[grep("id.incident", names(link.waze.edt))] = "uuid.waze"
  names(link.waze.edt)[grep("id.accident", names(link.waze.edt))] = "ID"
  
  # save the merged file as CSV and RData versions, to temporary output directory 
  write.csv(link.waze.edt, file=file.path(temp.outputdir, paste0("merged.waze.edt.", i,"_", HEXSIZE, "mi_MD.csv")), row.names = F)
  
  save(list=c("link.waze.edt", "edt.df"), file = file.path(temp.outputdir, paste0("merged.waze.edt.", i,"_", HEXSIZE, "mi_MD.RData")))
  
  if(CHECKPLOT) { 
    points(link.waze.edt$lon, link.waze.edt$lat, col = "blue"); 
    dev.print(jpeg, file = file.path(volpewazedir, paste0("Figures/Checking2_EDT_Waze_UAoverlay_", i, ".jpg")), width = 500, height = 500) 
  }  
} # End month loop ----

# move files out of temporary output directory to shared drive. This function will move only files with the pattern 'merged' in the file name, from the tempdir to the outputdir. Files are deleted after copying. Temporary directories are additionaly deleted automatically on restart.
movefiles(dir(temp.outputdir)[grep("merged", dir(temp.outputdir))], temp.outputdir, outputdir)

# Scratch from previous work

# # hexagons matching a Waze event -- 
# 
# any.waze.hex <- as.character(unique(unlist(d@data[gridcols])))
# # remove NA 
# any.waze.hex <- any.waze.hex[-which(is.na(any.waze.hex))]
# 
# 
# hex.md <- hex2[match(any.waze.hex, hex2$GRID_ID),]
# 
# # all hexagons
# plot(hex2)
# plot(hex.md, col = "blue", add = T)
# 
# save(list = c('hex.md', 'hex2'), file = file.path(volpewazedir, "spatial_layers/MD_hex.RData"))


# Read in link file. Three versions: from R script using time point, from Python script, and from R, using wider time window for Waze events. 
link1 <- read.csv(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/EDT_Waze_link_April_MD-timepoint.csv"))
link2 <- read.csv(file.path(volpewazedir, "output_mmg/link_table_edt_waze.txt"), sep = "\t")
link <- read.csv(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/EDT_Waze_link_April_MD.csv"))
linkww <- read.csv(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/Waze_Waze_link_April_MD.csv"))
linkee <- read.csv(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/EDT_EDT_link_April_MD.csv"))

# EDT_Waze_link_April_MD-timepoint.csv file (used single timepoint instead of window)
# compare pairs
link1.p <- paste(link1[,1], link1[,2])
link2.p <- paste(link2[,1], link2[,2])
link3.p <- paste(link[,1], link[,2])

link1.p <- sort(link1.p)
link2.p <- sort(link2.p)
link3.p <- sort(link3.p)

identical(length(link1.p), length(link2.p)) # identical!!
length(link3.p) - length(link1.p) # additional linked records
summary(link1.p == link2.p) # identical!


# <><><><><><><><><><><><>
# Waze-Waze ----
# <><><><><><><><><><><><>

# Join Waze incident data to link table (full join)
link.waze <- full_join(linkww, waze.df, by=c("id.accident"="uuid")) 

# Add Wa code to match column to indicate only Waze accident only data
link.waze$match <- ifelse(is.na(link.waze$match), 'Wa', link.waze$match)

# Join Waze accident data to Waze-link table (full join). X variables for Waze accidents, Y variables for Waze incidents
link.waze.waze <- full_join(linkww, waze.df, by = c("id.incidents"="uuid")) 

#Add Wu code to match column to indicate only Waze incident only data
link.waze.waze$match <- ifelse(is.na(link.waze.waze$match), 'Wi', link.waze.waze$match)
table(link.waze.waze$match)


#save the merged file as a csv file
write.csv(link.waze.waze, file=file.path(outputdir, "merged.waze.waze.April_MD.csv"), row.names = F)

#Save the merged Rdata file as an Rdata file
saveRDS(link.waze.waze, file = file.path(outputdir, "merged.waze.waze.April_MD.rds"))
