# Bellevue data prep for spatial filtering in ArcMap
# Start from compiled data created in reports/Hot_spot_Bellevue.Rmd


user <- paste0( "/home/", system("whoami", intern = TRUE)) #the user directory to use
localdir <- paste0(user, "/workingdata") # full path for readOGR
state = "WA"
use.tz <- "US/Pacific"
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"

load(file.path(localdir, "WA", "Waze", "WA_Bellevue_Prep.RData"))

# Two sets of data frames: w.all / w.all.proj and waze.ll / waze.ll.proj. *.proj have lat long in USGS Albers Equal Area.
# waze.ll has a small subset of variables, including lat long and time stamp, but no alert_uuid.

# check for identical rows, since we don't have alert_uuid to match on
ll.hash = with(waze.ll, paste(lat, lon, alert_type, roadclass))
all.hash = with(w.all, paste(lat, lon, alert_type, road_type))

stopifnot(identical(ll.hash, all.hash))

# add: subtype, city, street, and direction of travel (magvar)

waze.ll <- data.frame(waze.ll, w.all[c("sub_type", "city", "street", "magvar")])

fn.export = paste(state, 
                  "Bellevue_Prep_Export", sep="_")

save(list = c("zoom_box", "zoomll",
              "waze.ll", "waze.ll.proj",
              "zoom_box.ll"),
     file = file.path(localdir, "WA",
                      paste0(fn.export,'.RData')))

write.csv(waze.ll, file = file.path(localdir, "WA", paste0(fn.export,'.csv')), row.names = F)

zipname = paste0('Mapping_Bellevue_prep_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "WA", paste0(fn.export,'.RData')),
             file.path(localdir, "WA", paste0(fn.export,'.csv'))
             ))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))
