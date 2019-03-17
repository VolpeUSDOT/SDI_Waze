# Setting up workstation in SDC

# Make directories to work in:
# - ~/workingdata especially, this is where we can pull things down from S3 team bucket and push outputs back up to S3 

# Pull key data files from S3 bucket, including:
# - Shapefiles (census counties, hex tesselations)
# - EDT data
# - Prepared Waze data that is the output of the datacleaning steps

# Main idea: pull files from our permanent storage in S3 to the temporary working data directory on the user's analysis EC2 instance home directory.
# Syntax is aws s3 cp <teambucket/file/path> <local/file/path>
# Pass this to the terminal via R using the system() command

# Simple case: system('aws s3 cp s3://prod-sdc-sdi-911061262852-us-east-1-bucket/TestResults.csv ~/workingdata/TestResults.csv')

# More flexible: create copy commands by pasting together the team bucket name, directory, and file name as the source, and ~/workingdata, sub-directory, and file name as destination.

GETOUTPUT = F # Set to T to get Random Forest Output, leave as F to save space

# Create directory structure ----

toplevel = c('workingdata', 'agg_out', 'tempout')

for (i in toplevel){
  if(length(dir(file.path("~", i)))==0) system(paste('mkdir -p', file.path("~", i)))
}

# Create directories within 'workingdata' 

workinglevel = c('census', 'EDT', 'Figures', 'Hex', 'Link', 'Overlay', 'Random_Forest_Output',
                 'AADT', 'FARS', 'LODES_LEHD', 'SpecialEvents', 'Weather')

for (i in workinglevel){
  system(paste('mkdir -p', file.path("~", "workingdata", i)))
}

# Populate with S3 contents ----

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

states = c("CT", "MD", "TN", "UT", "VA", "WA")


for (i in states){
  system(paste('mkdir -p', file.path("~", "workingdata", i)))
}

# Waze data ----
# Grab any <state>_<yyyy-mm>.RData files from S3 and place in appropriate state directory on local


for(state in states){
  Waze.ls <- system(paste("aws s3 ls", 
                             file.path(teambucket, paste0(state, "/"))
  ),
  intern = T)
  
  # parse to file names
  Waze.ls <- unlist(lapply(strsplit(Waze.ls, " "), function(x) x[[length(x)]]))
  
  Waze.ls <- Waze.ls[grep(paste0("^", state, "_"), Waze.ls)] # get just Waze files: starts with <state>_
  Waze.ls <- Waze.ls[grep("RData$", Waze.ls)] # ends with RData
  Waze.ls <- Waze.ls[nchar(Waze.ls)==16] # is 16 characters long 
  
  for(i in Waze.ls){
    if(length(grep(i, dir(file.path('~', 'workingdata', state, 'Waze'))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, state, i),
                   file.path('~', 'workingdata', state, 'Waze', i)))
    }
  }
}



# census ----

# cb_2016_us_ua10_500k
# cb_2017_us_county_500k

# Get all contents 

census.ls <- system(paste("aws s3 ls", 
             file.path(teambucket, "census/")
             ),
       intern = T)

# parse to file names
census.ls <- unlist(lapply(strsplit(census.ls, " "), function(x) x[[length(x)]]))

for(i in census.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'census'))))==0){
    system(paste("aws s3 cp",
               file.path(teambucket, 'census', i),
               file.path('~', 'workingdata', 'census', i)))
  }
}

# EDT ----

edt.ls = c("EDTsubset_april2017_to_present.zip",
              "Maryland_april2017_to_present.csv",
           'CTMDUTVA_20170401_20180731.txt')

for(i in edt.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'EDT'))))==0){
    
    system(paste("aws s3 cp",
               file.path(teambucket, 'EDT', i),
               file.path('~', 'workingdata', 'EDT', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'EDT', i), '-d',
                                              file.path('~', 'workingdata', 'EDT/')))
  }
}

# Rename Maryland to MD

# system(paste('mv ~/workingdata/EDT/Maryland_april2017_to_present.csv ~/workingdata/EDT/MD_april2017_to_present.csv'))

# Hex ----

hex.ls <- system(paste("aws s3 ls", 
                          file.path(teambucket, "Hex/")
),
intern = T)

# parse to file names
# hex.ls <- unlist(lapply(strsplit(hex.ls, " "), function(x) x[[length(x)]]))
# 
# hex.ls <- hex.ls[!1:length(hex.ls) %in% grep("/", hex.ls)] # Omit directories

hex.ls <- c("ct_md_ut_va_hexagons_1mi.zip")

for(i in hex.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'Hex'))))==0){
   system(paste("aws s3 cp",
               file.path(teambucket, 'Hex', i),
               file.path('~', 'workingdata', 'Hex', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'Hex', i), '-d',
                                              file.path('~', 'workingdata', 'Hex/')))
  }
}


# And again, just for MD (with multiple grid sizes)
# 
# hex.ls <- system(paste("aws s3 ls", 
#                        file.path(teambucket, "Hex/MD_hexagons_shapefiles/")
# ),
# intern = T)
# 
# # parse to file names
# hex.ls <- unlist(lapply(strsplit(hex.ls, " "), function(x) x[[length(x)]]))
# 
# hex.ls <- hex.ls[!1:length(hex.ls) %in% grep("/", hex.ls)] # Omit directories
# 
# for(i in hex.ls){
#   if(length(grep(i, dir(file.path('~', 'workingdata', 'Hex', 'MD_hexagons_shapefiles'))))==0){
#     
#   system(paste("aws s3 cp",
#                file.path(teambucket, 'Hex', 'MD_hexagons_shapefiles', i),
#                file.path('~', 'workingdata', 'Hex', 'MD_hexagons_shapefiles', i)))
#   if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workittngdata', 'Hex', 'MD_hexagons_shapefiles', i),
#                                               '-d', file.path('~', 'workingdata', 'Hex/')))
#   }
# }

# Link ----


Link.ls <- system(paste("aws s3 ls", 
                       file.path(teambucket, "Link/")
),
intern = T)

# parse to file names
Link.ls <- unlist(lapply(strsplit(Link.ls, " "), function(x) x[[length(x)]]))

Link.ls <- Link.ls[!1:length(Link.ls) %in% grep("/", Link.ls)] # Omit directories

for(i in Link.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'Link'))))==0){
    
  system(paste("aws s3 cp",
               file.path(teambucket, 'Link', i),
               file.path('~', 'workingdata', 'Link', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'Link', i), -'d',
                                              file.path('~', 'workingdata', 'Link/')))
  }  
}


# Overlay ----

# Within state folders

for(state in states){
  Overlay.ls <- system(paste("aws s3 ls", 
                          file.path(teambucket, paste0(state, "/"))
  ),
  intern = T)
  
  # parse to file names
  Overlay.ls <- unlist(lapply(strsplit(Overlay.ls, " "), function(x) x[[length(x)]]))
  
  Overlay.ls <- Overlay.ls[grep("^merged.waze.edt", Overlay.ls)] # get just merge files
  
  for(i in Overlay.ls){
    if(length(grep(i, dir(file.path('~', 'workingdata', 'Overlay'))))==0){
      
     system(paste("aws s3 cp",
                 file.path(teambucket, state, i),
                 file.path('~', 'workingdata', 'Overlay', i)))
    if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'Overlay', i)))
    }
  }
}

# Random_Forest_Output --- 
GETOUTPUT = F 

if(GETOUTPUT){

for(state in states){
  Random_Forest_Output.ls <- system(paste("aws s3 ls", 
                             file.path(teambucket, state, "RandomForest_Output/")
  ),
  intern = T)
  
  # parse to file names
  Random_Forest_Output.ls <- unlist(lapply(strsplit(Random_Forest_Output.ls, " "), function(x) x[[length(x)]]))
  
  Random_Forest_Output.ls <- Random_Forest_Output.ls[grep("RandomForest_Output.RData$", Random_Forest_Output.ls)] # get just RF
  
  for(i in Random_Forest_Output.ls){
    if(length(grep(i, dir(file.path('~', 'workingdata', 'Random_Forest_Output'))))==0){
      
    system(paste("aws s3 cp",
                 file.path(teambucket, state, i),
                 file.path('~', 'workingdata', 'Random_Forest_Output', i)))
    if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'Random_Forest_Output', i)))
    }
  }
}

}
# AADT ----


aadt.ls = c('AADT_CT_MD_UT_VA.zip')
  
for(i in aadt.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'AADT'))))==0){
    
  system(paste("aws s3 cp",
               file.path(teambucket, 'AADT', i),
               file.path('~', 'workingdata', 'AADT', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'AADT', i),
                                              '-d', file.path('~', 'workingdata', 'AADT/')))
  }
}


# LODES_LEHD ----


lodes.ls = c('LODES_LEHD_CT_MD_UT_VA.zip')

for(i in lodes.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'LODES_LEHD'))))==0){
    
  system(paste("aws s3 cp",
               file.path(teambucket, 'LODES_LEHD', i),
               file.path('~', 'workingdata', 'LODES_LEHD', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'LODES_LEHD', i), '-d',
                                              file.path('~', 'workingdata', 'LODES_LEHD/')))
  }
}

# FARS ----

fars.ls = c('FARS_CT_MD_UT_VA.zip')

for(i in fars.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'FARS'))))==0){
    
  system(paste("aws s3 cp",
               file.path(teambucket, 'FARS', i),
               file.path('~', 'workingdata', 'FARS', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'FARS', i), '-d',
                                              file.path('~', 'workingdata', 'FARS/')))
  }
}


# Special Events ----

special.ls = c('SpecialEvents.zip')

for(i in special.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'SpecialEvents'))))==0){
    
    system(paste("aws s3 cp",
                 file.path(teambucket, 'SpecialEvents', i),
                 file.path('~', 'workingdata', 'SpecialEvents', i)))
    if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'SpecialEvents', i), '-d',
                                                file.path('~', 'workingdata', 'SpecialEvents/')))
  }
}

# Weather ----

# GHCN data 

wx.ls <- system(paste("aws s3 ls", 
                          file.path(teambucket, "Weather/")
),
intern = T)

wx.ls <- unlist(lapply(strsplit(wx.ls, " "), function(x) x[[length(x)]]))

for(i in wx.ls){
  if(length(grep(i, dir(file.path('~', 'workingdata', 'Weather'))))==0){
    system(paste("aws s3 cp",
                 file.path(teambucket, 'Weather', i),
                 file.path('~', 'workingdata', 'Weather', i)))
    if(length(grep('zip$', i))!=0) {
      system(paste('unzip -o', file.path('~', 'workingdata', 'Weather', i), '-d',
                   file.path('~', 'workingdata', 'Weather/')))
    }   
  }
}

# Tennessee data ----

tn.ls = c('TN.zip', 'Weather/TN_Weather_GHCN.zip', 
          'Shapefiles/TN_Roadway_Shapefiles.zip', 'SpecialEvents/TN_SpecialEvent_2018.RData', 
          'Shapefiles/timezones.shapefile.zip', "SpecialEvents/TN_SpecialEvent_2017.RData",
          'Crash/TITAN_Crash_181108.zip', 'Crash/TN_Crash_Simple_2008-2018.RData')

for(i in tn.ls){
  subdir = as.character(lapply(strsplit(i, '/'), function(x) x[1]))
  fileinsubdir = as.character(lapply(strsplit(i, '/'), function(x) x[2]))
  
  if(!is.na(fileinsubdir)){
    
    if(length(grep(fileinsubdir, dir(file.path('~', 'workingdata', 'TN', subdir))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, 'TN', i),
                   file.path('~', 'workingdata', 'TN', i)))
      if(length(grep('zip$', i))!=0) {
        system(paste('unzip -o', file.path('~', 'workingdata', 'TN', i), '-d',
                     file.path('~', 'workingdata', 'TN', paste0(subdir, '/'))))
      }
    }
    # If no sub directory:
  } else {
    if(length(grep(i, dir(file.path('~', 'workingdata', 'TN'))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, 'TN', i),
                   file.path('~', 'workingdata', 'TN', i)))
      if(length(grep('zip$', i))!=0) {
        system(paste('unzip -o', file.path('~', 'workingdata', 'TN', i), '-d',
                     file.path('~', 'workingdata', 'TN/')))
      }    
      
    }
  } # end if else for subdirectory
}

# Bellevue data ----

wa.ls = c('Shapefiles/Bellevue_Roadway.zip', 'Crashes/Bellevue_Crash.zip', "Roadway/RoadNetwork_Jurisdiction.csv", "Weather/Bellevue_2018_GHCN_Weather.zip")

for(i in wa.ls){
  subdir = as.character(lapply(strsplit(i, '/'), function(x) x[1]))
  fileinsubdir = as.character(lapply(strsplit(i, '/'), function(x) x[2]))
  
  if(!is.na(fileinsubdir)){
    
    if(length(grep(fileinsubdir, dir(file.path('~', 'workingdata', 'WA', subdir))))==0){
      
      system(paste("aws s3 cp",
                   file.path(teambucket, 'WA', i),
                   file.path('~', 'workingdata', 'WA', i)))
      if(length(grep('zip$', i))!=0) {
          system(paste('unzip -o', file.path('~', 'workingdata', 'WA', i), '-d',
                     file.path('~', 'workingdata', 'WA', paste0(subdir, '/'))))
      }
    }
    # If no sub directory:
    } else {
      if(length(grep(i, dir(file.path('~', 'workingdata', 'WA'))))==0){
        
        system(paste("aws s3 cp",
                     file.path(teambucket, 'WA', i),
                     file.path('~', 'workingdata', 'WA', i)))
        if(length(grep('zip$', i))!=0) {
                system(paste('unzip -o', file.path('~', 'workingdata', 'WA', i), '-d',
                       file.path('~', 'workingdata', 'WA/')))
        }    
      
    }
  } # end if else for subdirectory
}

# Reorganize unzipped directories if necessary
if(length(grep("TN$", dir(file.path('~', 'workingdata', 'TN'))))>0) {
  for(i in c("Crash", "Output", "SpecialEvents", "Weather")){
    system(paste0('mv ~/workingdata/TN/TN/', i, ' ~/workingdata/TN/', i))
  }
  system(paste0('rm ~/workingdata/TN/TN/ -R'))
}

# Re-organizing export of model outputs for new system ----
#  This can serve as example code snippet to place within any analysis script, to save outputs first to the team bucket and then exportable outputs to the export_requests directory in the team bucket.

EXPORTREORG = F

if(EXPORTREORG){
  system(paste("aws s3 ls", file.path(teambucket, 'export_requests/')))
  
  
  system(paste("aws s3 ls", file.path(teambucket, 'TN', 'Daily_Weather_Prep/')))
  system(paste("aws s3 cp", 
               file.path(teambucket, 'TN', 'Daily_Weather_Prep'),
               file.path(teambucket, 'TN', 'Daily_Weather_Prep_prev'),
               '--recursive'
               ))
  
  system(paste("aws s3 rm", 
               file.path(teambucket, 'TN', 'Daily_Weather_Prep'),
               '--recursive'
  ))
  
  # re-copy some files 
  system(paste("aws s3 cp", 
               file.path(teambucket, 'export_requests', 'Hot_Spot_Mapping_Multiple_Figures_RasterLayers_2018-11-20.zip'),
               file.path(teambucket, 'export_requests', 'Hot_Spot_Mapping_Multiple_Figures_RasterLayers_2018-11-21.zip')))
  
  # "Hot_Spot_Mapping_Bellevue_prep_2018-11-19.zip"
  system(paste("aws s3 ls", file.path(teambucket, 'MD/')))
 
  
  outputdir = '~/workingdata/Random_Forest_Output'
  
  # Using system zip:
  # system(paste('zip ~/workingdata/zipfilename.zip ~/path/to/your/file'))
  
  # More simple, use R wrapper:
  # zip(zipfilename, files = c(file1, file2))
  
  zipname = 'RandomForest_Outputs_2018-09-18.zip'
  
  system(paste('zip', file.path('~/workingdata', zipname),
    file.path(outputdir, 'MD_VMT_Output_to_30.RData'),
    file.path(outputdir, 'CT_VMT_Output_to_30.RData'),
    file.path(outputdir, 'MD_Model_63_RandomForest_Output.RData'),
    file.path(outputdir, 'MD_Model_62_RandomForest_Output.RData'),
    file.path(outputdir, 'MD_Model_61_RandomForest_Output.RData'),
    file.path(outputdir, 'MD_Model_30_RandomForest_Output.RData'),
    file.path(outputdir, 'CT_Model_63_RandomForest_Output.RData'),
    file.path(outputdir, 'CT_Model_62_RandomForest_Output.RData'),
    file.path(outputdir, 'CT_Model_61_RandomForest_Output.RData'),
    file.path(outputdir, 'CT_Model_30_RandomForest_Output.RData')
  ))

  system(paste(
    'aws s3 cp',
    file.path('~/workingdata', zipname),
    file.path(teambucket, 'export_requests', zipname)
  ))
  
  
}

# Check contents of upload directory

CHECKUPLOAD = F

if(CHECKUPLOAD){
  system(paste(
    'aws s3 ls',
    file.path(teambucket, system('whoami', intern = T), 'uploaded_files/')
  ))
  
}
# Moving from upload ----

MOVEFROMUPLOAD = F

if(MOVEFROMUPLOAD){
 
  system(paste("aws s3 ls", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files/")
  ))

  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "CTMDUTVA_2018_Weather.zip"),
               file.path(teambucket, "Weather", "CTMDUTVA_2018_Weather.zip"))
  )

    system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "Bellevue_2018_GHCN_Weather.zip"),
               file.path(teambucket, "WA", "Weather", "Bellevue_2018_GHCN_Weather.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TITAN_Crash_181108.zip"),
               file.path(teambucket, "TN", "Crash", "TITAN_Crash_181108.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "timezones.shapefile.zip"),
               file.path(teambucket, "TN", "Shapefiles", "timezones.shapefile.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "Bellevue_Roadway.zip"),
               file.path(teambucket, "WA", "Shapefiles", "Bellevue_Roadway.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "Bellevue_Crash.zip"),
               file.path(teambucket, "WA", "Crashes", "Bellevue_Crash.zip"))
  )

  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TN_Roadway_Shapefiles.zip"),
               file.path(teambucket, "TN", "Shapefiles", "TN_Roadway_Shapefiles.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TN_Weather_GHCN.zip"),
               file.path(teambucket, "TN", "Weather", "TN_Weather_GHCN.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TN.zip"),
               file.path(teambucket, "TN", "TN.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "AADT_CT_MD_UT_VA.zip"),
               file.path(teambucket, "AADT", "AADT_CT_MD_UT_VA.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "FARS_CT_MD_UT_VA.zip"),
               file.path(teambucket, "FARS", "FARS_CT_MD_UT_VA.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "LODES_LEHD_CT_MD_UT_VA.zip"),
               file.path(teambucket, "LODES_LEHD", "LODES_LEHD_CT_MD_UT_VA.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "ct_md_ut_va_hexagons_1mi.zip"),
               file.path(teambucket, "Hex", "ct_md_ut_va_hexagons_1mi.zip"))
  )
  
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "SpecialEvents.zip"),
               file.path(teambucket, "SpecialEvents", "SpecialEvents.zip"))
  )
  
  # Jessie add 2018 TN special event data
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TN_SpecialEvent_2018.RData"),
               file.path(teambucket, "TN", "SpecialEvents", "TN_SpecialEvent_2018.RData"))
  )

  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "TN_SpecialEvent_2017.RData"),
               file.path(teambucket, "TN", "SpecialEvents", "TN_SpecialEvent_2017.RData"))
  )
  
  #Erika Files
  system(paste("aws s3 mv", 
               file.path(teambucket, system('whoami', intern = T), "uploaded_files", "RoadNetwork_Jurisdiction.csv"),
               file.path(teambucket, "WA", "Roadway", "RoadNetwork_Jurisdiction.csv"))
  )
  
  
}

# Team bucket organization ----
# one time work, saving here for posterity

REORG = F

if(REORG){

system(paste("aws s3 ls", teambucket))

  system(paste("aws s3 ls", file.path(teambucket, 'Test_from_Dan/')))
  system(paste("aws s3 rm", file.path(teambucket, 'Test_from_Dan')))
  
# Examples of how to remove individual files or folders
system(paste("aws s3 rm", 
             file.path(teambucket, 'FARS_CT_2015_2016_sum_fclass.shp')))

system(paste("aws s3 rm", file.path( teambucket, 'HomeFolderData/'), '--recursive'))

# Move multiple files
system(paste('aws s3 mv', 
             paste0(teambucket, "/"),
             file.path(teambucket, "MD/"),
             '--dryrun --include "MD_2017*"')
)


# Organize EDT data
system(paste("aws s3 mv", 
             file.path(teambucket, "Maryland_april2017_to_present.csv"),
             file.path(teambucket, "EDT", "Maryland_april2017_to_present.csv"))
       )


system(paste("aws s3 mv", 
             file.path(teambucket, "EDTsubset_april2017_to_present.zip"),
             file.path(teambucket, "EDT", "EDTsubset_april2017_to_present.zip"))
       )

system(paste("aws s3 mv", 
             file.path(teambucket, "CTMDUTVA_20170401_20180731.txt"),
             file.path(teambucket, "EDT", "CTMDUTVA_20170401_20180731.txt"))
)

system(paste("aws s3 mv", 
             file.path(teambucket, "vmt_max_aadt_by_grid_fc_urban_factored.txt"),
             file.path(teambucket, "AADT", "vmt_max_aadt_by_grid_fc_urban_factored.txt"))
)

system(paste("aws s3 mv", 
             file.path(teambucket, system('whoami', intern = T), "uploaded_files", "AADT_CT_MD_UT_VA.zip"),
             file.path(teambucket, "AADT", "AADT_CT_MD_UT_VA.zip"))
)

system(paste("aws s3 mv", 
             file.path(teambucket, system('whoami', intern = T), "uploaded_files", "FARS_CT_MD_UT_VA.zip"),
             file.path(teambucket, "FARS", "FARS_CT_MD_UT_VA.zip"))
)

# View uploaded files
system(paste("aws s3 ls", 
             file.path(teambucket, system('whoami', intern = T), "uploaded_files/")))

system(paste("aws s3 mv", 
             file.path(teambucket, system('whoami', intern = T), "uploaded_files", "LODES_LEHD_CT_MD_UT_VA.zip"),
             file.path(teambucket, "LODES_LEHD", "LODES_LEHD_CT_MD_UT_VA.zip"))
)

}
