# Assigns urban area classifications to Waze and EDT events
# Merges the two files into a matched data frame and saves

# <><><><><><><><><><><><><><><><><><><><>
# Setup ----
library(tidyverse)
library(sp)
library(maps) # for mapping base layers
library(reshape)
library(maptools) # readShapePoly() and others
library(mapproj) # for coord_map()
library(rgdal) # for readOGR(), needed for reading in ArcM shapefiles
library(rgeos) # for gIntersection, to clip two shapefiles
library(raster)

#Flynn drive
codedir <- "~/git/SDI_Waze" 
wazemonthdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
wazedir <- "W:/SDI Pilot Projects/Waze/"
volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
outputdir <- "W:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

#Sudderth drive
# wazemonthdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"
# wazedir <- "S:/SDI Pilot Projects/Waze/"
# volpewazedir <- "//vntscex.local/DFS/Projects/PROJ-OR02A2/SDI/"
# outputdir <- "S:/SDI Pilot Projects/Waze/MASTER Data Files/Waze Aggregated/month_MD_clipped"

source(file.path(codedir, "utility/wazefunctions.R")) # for movefiles

setwd(wazedir)

# Read in urban/rural layer from Census
# From: https://www.census.gov/geo/maps-data/data/cbf/cbf_ua.html
# https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshprd13/TGRSHPrd13_TechDoc_A.pdf

ua <- readOGR(file.path(wazedir, "Working Documents/Census Files"), layer = "cb_2016_us_ua10_500k")

# Read in hexagon shapefile. This is a rectangular surface of 1 sq mi area hexagons, 
hex <- readOGR(file.path(volpewazedir, "spatial_layers/MD_hexagons_shapefiles"), layer = "MD_hexagons_1mi")

# read in county shapefile

co <- readOGR(file.path(wazedir, "Working Documents/Census Files"), layer = "cb_2015_us_county_500k")

# EDT: See CrashFact in W:\SDI Pilot Projects\Volpe\2017_11_03\waze\input\edt\2016_01_to_2017_09_MD_and_IN.
# This has been reduced to just Maryland data from April 2017.
load(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/2017-04_1_CrashFact_edited.RData"))

# Waze: Comes from aggregated monthly Waze events, clipped to a 0.5 mile buffer around MD, for April 2017
load(file.path(wazedir, "MASTER Data Files/Waze Aggregated/month_MD_clipped/MD_buffered__2017-04.RData"))

# Read in link files. Three versions: from R script using time point, from Python script, and from R, using wider time window for Waze events. 
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

### Some code for subsetting EDT and Waze data based on link table

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

# <><><><><><><><><><><><><><><><><><><><>
# Start ua overlay

# Overlay points in polygons
# Use over() from sp to join these to the census polygon. Points in Polygons, pip

proj4string(d) <- proj4string(edt.april) <- proj4string(ua) 

waze_ua_pip <- over(d, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in d. 
edt_ua_pip <- over(edt.april, ua[,c("NAME10","UATYP10")]) # Match a urban area name and type to each row in edt.april. 

d@data <- data.frame(d@data, waze_ua_pip)
names(d@data)[(length(d@data)-1):length(d@data)] <- c("Waze_UA_Name", "Waze_UA_Type")

edt.april@data <- data.frame(edt.april@data, edt_ua_pip)
names(edt.april@data)[(length(edt.april@data)-1):length(edt.april@data)] <- c("EDT_UA_Name", "EDT_UA_Type")

# # lat/longs should be extremely close. Use EDT as the 'center' for each given row.
# with(ew[complete.cases(ew[,c('lat','GPSLat')]),], cor(lat, GPSLat))
# with(ew[complete.cases(ew[,c('lon','GPSLong_New')]),], cor(lon, GPSLong_New))

# <><><><><><><><><><><><><><><><><><><><>
# Hexagon overlay

# match coordinate reference system of hexagons to Urban Areas and counties
proj4string(hex)
proj4string(co)

# maryland FIPS = 24
md.co <- co[co$STATEFP == 24,]

hex2 <- spTransform(hex, proj4string(md.co))

# grid column names
gridcols <- names(hex2)[grep("^GRID", names(hex2))]

waze_hex_pip <- over(d, hex2[,gridcols]) # Match hexagon names to each row in d. 
edt_hex_pip <- over(edt.april, hex2[,gridcols]) # Match hexagon names to each row in edt.april. 

# add these columns to the data frame
d@data <- data.frame(d@data, waze_hex_pip)

edt.april@data <- data.frame(edt.april@data, edt_hex_pip)

# <><><><><><><><><><><><><><><><><><><><>
# Merge and save EDT-Waze
# <><><><><><><><><><><><><><><><><><><><>

# Save Waze data as dataframe
waze.df <- d@data #439,562 x 32
names(waze.df)

# Save EDT data as dataframe
edt.df <- edt.april@data #9,271 x 63
names(edt.df)

# before carrying out the join, rename the EDT grid cell columns 
names(edt.df)[grep("^GRID", names(edt.df))] <- paste(names(edt.df)[grep("^GRID", names(edt.df))], "edt", sep = ".")

# Join Waze data to link table (full join)
link.waze <- full_join(link, waze.df, by=c("uuid.waze"="uuid")) #442,500 x 34

# Add W code to match column to indicate only Waze data
link.waze$match <- ifelse(is.na(link.waze$match), 'W', link.waze$match)

# Join EDT data to Waze-link table (full join)

# *** Potential improvement for data storage: at this step, make a selection of only those columns which will be useful from EDT (do not include all 0 columns or columns not useful for the model) ***

edt.df$CrashDate_Local <- as.character(edt.df$CrashDate_Local) # will need to convert back to POSIX
link.waze.edt <- full_join(link.waze, edt.df, by = c("id.edt"="ID")) #446,498 x 96

# Check size - total should equal number non-matched EDT + number non-matched Waze + nrow(link table);
# 3998 + 413294 + 29206 = 446,498 (it matches!)

#Add E code to match column to indicate only EDT data
link.waze.edt$match <- ifelse(is.na(link.waze.edt$match), 'E', link.waze.edt$match)
table(link.waze.edt$match)

# Convert CrashDate_Local back to POSIX

link.waze.edt$CrashDate_Local <- strptime(link.waze.edt$CrashDate_Local, "%Y-%m-%d %H:%M:%S", tz = "America/New_York")

# Save files first to a local temporaroy directory, then move to the shared drive and delete local copies when complete.
temp.outputdir = tempdir() 

#save the merged file as a csv file
write.csv(link.waze.edt, file=file.path(temp.outputdir, "merged.waze.edt.April_MD.csv"), row.names = F)

#Save the merged Rdata file as an Rdata file
saveRDS(link.waze.edt, file = file.path(temp.outputdir, "merged.waze.edt.April_MD.rds"))

save(list=c("link.waze.edt", "edt.df"), file = file.path(temp.outputdir, "merged.waze.edt.April_MD.RData"))

movefiles(dir(temp.outputdir)[grep("merged", dir(temp.outputdir))], temp.outputdir, outputdir)

# hexagons matching a Waze event

any.waze.hex <- as.character(unique(unlist(d@data[gridcols])))
# remove NA 
any.waze.hex <- any.waze.hex[-which(is.na(any.waze.hex))]


hex.md <- hex2[match(any.waze.hex, hex2$GRID_ID),]

# all hexagons
plot(hex2)
plot(hex.md, col = "blue", add = T)

save(list = c('hex.md', 'hex2'), file = file.path(volpewazedir, "spatial_layers/MD_hex.RData"))


# <><><><><><><><><><><><>
# Waze-Waze
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
