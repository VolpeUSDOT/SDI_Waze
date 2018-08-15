# Get all necessary packages across data prep and analysis scripts for SDI Waze project
# If this fails, see "Package Installation Notes.Rmd" for details on tidyverse installation

loadpacks <- c("sp",
           "maps",
           "reshape",
           "maptools",
           "mapproj",  # for coord_map(", 
           "rgdal",  # for readOGR(", , needed for reading in ArcM shapefiles
           "rgeos",  # for gIntersection, to clip two shapefiles
           "raster", 
           "lubridate",
           "utils",
           "randomForest",
           "maptree", # for better graphing of decision trees
           "party",
           "partykit",
           "pROC",
           "foreach", # for parallel implementation
           "doParallel",
           "aws.s3", # AWS convenience functions s3save and s3load
           "tidyverse")


for(i in loadpacks){if(length(grep(i, (.packages(all.available=T))))==0) install.packages(i, dependencies =TRUE)}
