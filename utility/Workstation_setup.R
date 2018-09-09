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

# Create directory structure ----

toplevel = c('workingdata', 'agg_out', 'tempout')

for (i in toplevel){
  if(length(dir(file.path("~", i)))==0) system(paste('mkdir', file.path("~", i)))
}

# Create directories within 'workingdata' 

workinglevel = c('census', 'EDT', 'Figures', 'Hex', 'Link', 'Overlay', 'Random_Forest_Output',
                 'AADT', 'FARS', 'LODES_LEHD')

for (i in workinglevel){
  system(paste('mkdir -p', file.path("~", "workingdata", i)))
}

# Populate with S3 contents ----

teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

states = c("CT", "UT", "VA", "MD")

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
    system(paste("aws s3 cp",
               file.path(teambucket, 'census', i),
               file.path('~', 'workingdata', 'census', i)))
  }


# EDT ----

edt.ls = c("EDTsubset_april2017_to_present.zip",
              "Maryland_april2017_to_present.csv",
           'CTMDUTVA_20170401_20180731.txt')

for(i in edt.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'EDT', i),
               file.path('~', 'workingdata', 'EDT', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'EDT', i), '-d',
                                              file.path('~', 'workingdata', 'EDT/')))
  
}

# Rename Maryland to MD

system(paste('mv ~/workingdata/EDT/Maryland_april2017_to_present.csv ~/workingdata/EDT/Maryland_april2017_to_present.csv'))

# Hex ----

hex.ls <- system(paste("aws s3 ls", 
                          file.path(teambucket, "Hex/")
),
intern = T)

# parse to file names
hex.ls <- unlist(lapply(strsplit(hex.ls, " "), function(x) x[[length(x)]]))

hex.ls <- hex.ls[!1:length(hex.ls) %in% grep("/", hex.ls)] # Omit directories

for(i in hex.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'Hex', i),
               file.path('~', 'workingdata', 'Hex', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'Hex', i), '-d',
                                              file.path('~', 'workingdata', 'Hex/')))
  
}


# And again, just for MD (with multiple grid sizes)

hex.ls <- system(paste("aws s3 ls", 
                       file.path(teambucket, "Hex/MD_hexagons_shapefiles/")
),
intern = T)

# parse to file names
hex.ls <- unlist(lapply(strsplit(hex.ls, " "), function(x) x[[length(x)]]))

hex.ls <- hex.ls[!1:length(hex.ls) %in% grep("/", hex.ls)] # Omit directories

for(i in hex.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'Hex', 'MD_hexagons_shapefiles', i),
               file.path('~', 'workingdata', 'Hex', 'MD_hexagons_shapefiles', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'Hex', 'MD_hexagons_shapefiles', i),
                                              '-d', file.path('~', 'workingdata', 'Hex/')))
  
}

# Link ----


Link.ls <- system(paste("aws s3 ls", 
                       file.path(teambucket, "Link/")
),
intern = T)

# parse to file names
Link.ls <- unlist(lapply(strsplit(Link.ls, " "), function(x) x[[length(x)]]))

Link.ls <- Link.ls[!1:length(Link.ls) %in% grep("/", Link.ls)] # Omit directories

for(i in Link.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'Link', i),
               file.path('~', 'workingdata', 'Link', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'Link', i), -'d',
                                              file.path('~', 'workingdata', 'Link/')))
  
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
    system(paste("aws s3 cp",
                 file.path(teambucket, state, i),
                 file.path('~', 'workingdata', 'Overlay', i)))
    if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'Overlay', i)))
    
  }
}

# Random_Forest_Output --- 

for(state in states){
  Random_Forest_Output.ls <- system(paste("aws s3 ls", 
                             file.path(teambucket, paste0(state, "/"))
  ),
  intern = T)
  
  # parse to file names
  Random_Forest_Output.ls <- unlist(lapply(strsplit(Random_Forest_Output.ls, " "), function(x) x[[length(x)]]))
  
  Random_Forest_Output.ls <- Random_Forest_Output.ls[grep("RandomForest_Output.RData$", Random_Forest_Output.ls)] # get just RF
  
  for(i in Random_Forest_Output.ls){
    system(paste("aws s3 cp",
                 file.path(teambucket, state, i),
                 file.path('~', 'workingdata', 'Random_Forest_Output', i)))
    if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'Random_Forest_Output', i)))
    
  }
}

# AADT ----


aadt.ls = c('vmt_max_aadt_by_grid_fc_urban_factored.txt', 'AADT_CT_MD.zip')
  
for(i in aadt.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'AADT', i),
               file.path('~', 'workingdata', 'AADT', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip -o', file.path('~', 'workingdata', 'AADT', i),
                                              '-d', file.path('~', 'workingdata', 'AADT/')))
  
}


# LODES_LEHD ----


lodes.ls = c('LODES_LEHD_CT_UT.zip')

for(i in lodes.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'LODES_LEHD', i),
               file.path('~', 'workingdata', 'LODES_LEHD', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'LODES_LEHD', i), '-d',
                                              file.path('~', 'workingdata', 'LODES_LEHD/')))
  
}

# FARS ----



fars.ls = c('FARS_CT_UT.zip')

for(i in fars.ls){
  system(paste("aws s3 cp",
               file.path(teambucket, 'FARS', i),
               file.path('~', 'workingdata', 'FARS', i)))
  if(length(grep('zip$', i))!=0) system(paste('unzip', file.path('~', 'workingdata', 'FARS', i), '-d',
                                              file.path('~', 'workingdata', 'FARS/')))
}
  

# Team bucket organization ----
# one time work, saving here for posterity

REORG = F

if(REORG){

system(paste("aws s3 ls", teambucket))

  
system(paste("aws s3 rm", 
             file.path(teambucket, 'FARS_CT_2015_2016_sum_fclass.shp')))
  
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
             file.path(teambucket, "AADT_CT_MD.zip"),
             file.path(teambucket, "AADT", "AADT_CT_MD.zip"))
)

system(paste("aws s3 mv", 
             file.path(teambucket, "FARS_CT_UT.zip"),
             file.path(teambucket, "FARS", "FARS_CT_UT.zip"))
)

system(paste("aws s3 mv", 
             file.path(teambucket, "LODES_LEHD_CT_UT.zip"),
             file.path(teambucket, "LODES_LEHD", "LODES_LEHD_CT_UT.zip"))
)

}