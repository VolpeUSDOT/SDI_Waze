# Get all necessary packages across data prep and analysis scripts for SDI Waze project
# If this fails, see "Package Installation Notes.Rmd" for details on tidyverse installation

loadpacks <- c(
            "aws.s3", # AWS convenience functions s3save and s3load
            "doParallel",
            "DT",
            "foreach", # for parallel implementation
            "getPass",
            "kableExtra",
            "lubridate",
            "maps",
            "maptools",
            "mapproj",  # for coord_map(", 
            "maptree", # for better graphing of decision trees
            "pander",
            "party",
            "partykit",
            "pROC",
            "randomForest",
            "raster", 
            "rgdal",  # for readOGR(", , needed for reading in ArcM shapefiles
            "rgeos",  # for gIntersection, to clip two shapefiles
            "sp",
            "tidyverse",
            "utils")


for(i in loadpacks){if(length(grep(i, (.packages(all.available=T))))==0) install.packages(i, dependencies =TRUE)}
