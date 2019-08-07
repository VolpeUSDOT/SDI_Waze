# Output grided data for Tennessee

# Setup ---- 
rm(list=ls()) # Start fresh
library(tidyverse)

# codeloc <- "~/git/SDI_Waze" 
codeloc <- "~/SDI_Waze" 

source(file.path(codeloc, 'utility/get_packages.R')) # installs necessary packages

# localdir <- "//vntscex.local/DFS/Projects/PROJ-OS62A1/SDI Waze Phase 2/Output/TN"
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

user <- paste0( "/home/", system("whoami", intern = TRUE)) 
localdir <- paste0(user, "/workingdata/TN") # full path for readOGR

setwd(localdir)

# read utility functions, including movefiles
source(file.path(codeloc, 'utility/wazefunctions.R'))

grids = c("TN_01dd_fishnet", "TN_1sqmile_hexagons")

state = "TN"
wazemonthdir <- "~/workingdata/TN/Overlay" # contains the grid.overlay.waze.tn.YYYY-mm_<state>.RData files
temp.outputdir = "~/agg_out" # contains the WazeOnlyHexTimeList_YYYY-mm_grids_<state>.RData files; populate from S3 if necessary


# Make yearly csvs ----
for(g in grids){ # g = grids[1]
  
  mergefiles <- dir(wazemonthdir)[grep("^grid.overlay.waze.tn", dir(wazemonthdir))]
  gridmergefiles <- mergefiles[grep(g, mergefiles)]
  
  avail.months = substr(unlist(
    lapply(strsplit(gridmergefiles, "_"), function(x) x[[4]])), 1, 7)
  
  compiled_months <- vector()
  for(mo in avail.months){ 
    # mo = '2018-12'
    fn = paste("WazeOnlyTimeHexAll_", mo,"_", g, ".RData", sep="")
    load(file.path(temp.outputdir, fn))
    # Write this month directly to csv
    # Also compile to single csv for all available months
    write.csv(wazeTime.tn.hexAll, 
              file = file.path(temp.outputdir, paste0(
                "WazeEvents_", mo,"_", g, ".csv")),
              row.names = F)
    
    compiled_months <- rbind(compiled_months, wazeTime.tn.hexAll)
  } 
  # Write out compiled months to csv
  write.csv(compiled_months, 
            file = file.path(temp.outputdir, paste0(
              "WazeEvents_", avail.months[1],"_to_", avail.months[length(avail.months)],
              "_", g, ".csv")),
            row.names = F)
}

# Bundle data for exort ---- 

for(g in grids){ # g = grids[1]

  query = paste0('WazeEvents_\\d{4}-\\d{2}_', g, '.csv')
  month_csv <- dir(temp.outputdir)[grep(query, dir(temp.outputdir))]
  
  query = paste0('WazeEvents_\\d{4}-\\d{2}_to_\\d{4}-\\d{2}_', g, '.csv')
  compiled_csv <- dir(temp.outputdir)[grep(query, dir(temp.outputdir))]
  
  zipname_month = paste0("Monthly_Waze_", g, '_', Sys.Date(), '.zip')
  zipname_compiled = paste0("Compiled_Waze_", g, '_', Sys.Date(), '.zip')
  
  outfiles = file.path(temp.outputdir, month_csv)
  system(paste('zip -j', file.path(temp.outputdir, zipname_month),
            paste(outfiles, collapse = " "))
  )
  
  system(paste('zip -j', file.path(temp.outputdir, zipname_compiled),
               paste(file.path(temp.outputdir, compiled_csv), collapse = " "))
  )
  
  system(paste(
    'aws s3 cp',
    file.path(temp.outputdir, zipname_month),
    file.path(teambucket, 'export_requests', zipname_month)
  ))
  
  system(paste(
    'aws s3 cp',
    file.path(temp.outputdir, zipname_compiled),
    file.path(teambucket, 'export_requests', zipname_compiled)
  ))
  
}


### Additional exports -----

# Export prepared special events, prepared historical crash, and prepared weather
# This will allow local running of RF models with exported, gridded data

zipname = paste0('TN_NonWaze_ModelInputs_', Sys.Date(), '.zip')

outfiles = c(file.path(localdir, 'Crash', 'Prepared_TN_Crash_TN_01dd_fishnet.RData'),
             file.path(localdir, 'Crash', 'Prepared_TN_Crash_TN_1sqmile_hexagons.RData'),
             file.path(localdir, 'SpecialEvents', 'Prepared_TN_SpecialEvent_TN_01dd_fishnet.RData'),
             file.path(localdir, 'SpecialEvents', 'Prepared_TN_SpecialEvent_TN_1sqmile_hexagons.RData'),
             file.path(localdir, 'Weather', 'Prepared_Weather_TN_01dd_fishnet.RData'),
             file.path(localdir, 'Weather', 'Prepared_Weather_TN_1sqmile_hexagons.RData')
)

system(paste('zip -j', file.path(temp.outputdir, zipname),
             paste(outfiles, collapse = " "))
)

system(paste(
  'aws s3 cp',
  file.path(temp.outputdir, zipname),
  file.path(teambucket, 'export_requests', zipname)
))

