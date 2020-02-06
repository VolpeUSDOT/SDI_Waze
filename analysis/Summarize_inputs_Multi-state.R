# Summarize data used for the multi-state work

# Goal: Make one table that reports the following for the four states of MD, CT, VA, and UT for 2018.

# Total Waze counts and Waze accident counts
# in ~/workingdata, have organized data in each state's folder, with <state>_<yyyy>-<mm>.RData as the outputs from ReduceWaze_SDC.R, the first step in the data pipeline.


user <- paste0( "/home/", system("whoami", intern = TRUE)) # the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR

outputdir <- file.path(localdir, "Random_Forest_Output")

setwd(localdir)
# <><><><><>
states = c('CT', 'MD', 'UT', 'VA') #'CT'  #'UT' # Sets the state. UT, VA, MD are all options.
# <><><><><>

# Manually setting months to run here; could also scan S3 for months available for this state
do.months = c(#paste("2017", c("04","05","06","07","08","09", "10", "11", "12"), sep="-"),
  paste("2018", formatC(1:12, width = 2, flag = 0), sep="-"))

# Loop over states ----

sumtab <- vector()

for(state in states){
  
  # View the files available in S3 for this state: system(paste0('aws s3 ls ', teambucket, '/', state, '/'))
  Waze_Prepared_Data = paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_2018')
  load(file.path(localdir, paste0(Waze_Prepared_Data, ".RData")))
  
  sumtab <- rbind(sumtab, c('State' = state,
                            'Waze_Total' = sum(w.allmonths$uniqueWazeEvents),
                            'Waze_Crash' = sum(w.allmonths$nWazeAccident),
                            'EDT_Crash' = sum(w.allmonths$uniqEDTreports),
                            'N_grids' = length(unique(w.allmonths$GRID_ID)),
                            'N_total' = nrow(w.allmonths))
                  )
  
  print(sumtab)
  
}

(sumtab <- as.data.frame(sumtab))

#   State Waze_Total Waze_Crash EDT_Crash N_grids N_total
# 1    CT    3350744     101745    108893    4502 1780165
# 2    MD    7835169     259518    117121    7612 3617348
# 3    UT    1028665      41427     38504    5950  625428
# 4    VA    9381757     317070    131420   20782 4595456