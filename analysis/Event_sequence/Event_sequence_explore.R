#### Queries Redshift database for CT
### subsets months to match Phase 1 analysis (April - August)
## Builds clusters around "Accident" alert types in Waze data 
# Initial clusters within 1 mile and 1 hr of Waze accident (on same "street")


# Check for package installations
#codeloc <- "~/SDI_Waze"
#source(file.path(codeloc, 'utility/get_packages.R')) #comment out unless needed for first setup, takes a long time to run

## Load libraries
library(tidyverse) # tidyverse install on SDC may require additional steps, see Package Installation Notes.Rmd 
library(lubridate)
library(doParallel)
library(foreach)
library(sp)

# Location of SDC SDI Waze team S3 bucket. Files will first be written to a temporary directory in this EC2 instance, then copied to the team bucket.
teambucket <- "s3://prod-sdc-sdi-911061262852-us-east-1-bucket"
 
# aws.s3::get_bucket(output.loc) # forbidden 403, as aws.s3 relies on credentials, which we are not using. see: aws.signature::locate_credentials()
user <- paste0( "/home/", system("whoami", intern = TRUE)) #he user directory to use
output.loc <- paste0(user, "/tempout")

localdir <- paste0(user, "/workingdata/") # full path for dumping intermediate outputs
codeloc <- "~/SDI_Waze"

# read functions from utility files (just in case)
source(file.path(codeloc, 'utility/wazefunctions.R'))

# Query parameters
# states with available EDT data for model testing
states = c("CT", "UT", "VA", "MD")

# Time zone picker:
tzs <- data.frame(states, 
                  tz = c("US/Eastern",
                         "US/Mountain",
                         "US/Eastern",
                         "US/Eastern"),
                  stringsAsFactors = F)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Compiling by State ----
# <><><><><><><><><><><><><><><><><><><><><><><><>

# Get year/month, year/month/day, and last day of month vectors to create the SQL queries
yearmonths = c(
  paste(2017, formatC(4:12, width = 2, flag = "0"), sep="-"),
  paste(2018, formatC(1:7, width = 2, flag = "0"), sep="-")
)
yearmonths.1 <- paste(yearmonths, "01", sep = "-")
lastdays <- days_in_month(as.POSIXct(yearmonths.1)) # from lubridate
yearmonths.end <- paste(yearmonths, lastdays, sep="-")

starttime <- Sys.time()


############# Get Waze data for states
## Keep only CT at this point
i = 'CT'

PREPNEW = F # Flag for preparing new data from scratch, or reading from already prepared data. 

if(PREPNEW) {  
  
  # Make connection to Redshift. Requires packages DBI and RPostgres entering username and password if prompted:
  source(file.path(codeloc, 'utility/connect_redshift_pgsql.R'))
  
  # read data by state, across all query months for processing
  
  alert_query <- paste0("SELECT * FROM dw_waze.alert WHERE state='", i,
                        "' AND pub_utc_timestamp BETWEEN to_timestamp('", yearmonths.1[1], 
                        " 00:00:00','YYYY-MM-DD HH24:MI:SS') AND to_timestamp('", yearmonths.end[length(yearmonths)], " 23:59:59','YYYY-MM-DD HH24:MI:SS')") # end query
  
## Data for CT 
    ct.data <- dbGetQuery(conn, alert_query) 
    
## Keep only April - August 2017
ct.data <- filter(ct.data, pub_utc_timestamp < "2017-09-01 00:00:00 UTC")

## convert numeric variables
ct.data$location_lat <- as.numeric(ct.data$location_lat)
ct.data$location_lon <- as.numeric(ct.data$location_lon)

## Keep only the highest version of UUID version
ct.data <- ct.data %>% group_by(alert_uuid) %>% filter(uuid_version==max(uuid_version, na.rm=TRUE))
ct.data <- ungroup(ct.data)

length(which(is.na(ct.data$street)==TRUE))
# remove missing street observations and filter on street (in sequence)
# disproprtionate number of these fall in "Accidents"
ct.data <- filter(ct.data, is.na(street)==FALSE)


# pull accidents only (for now), about 33K accidents
my.acc <- filter(ct.data, alert_type=="ACCIDENT")

    
save(list = c("ct.data", "my.acc"), file = file.path(localdir, "CT_Event_Sequence.RData"))

## array elements too large in vectorized form.
## Distance matrix and HACM impractical (also too greedy)


######################################################################
### Use vector processing to build successive lists

## distance first, all events within 1 mile for each accident (keeps row ID from ct.data for each accident)
dist.match <- function(pt){
  my.dist <- spDistsN1(as.matrix(ct.data[,c(20,19)], nrow=nrow(ct.data), ncol=2), 
                      pt, longlat=TRUE)
  which(my.dist*0.6213712 <= 1)
}

# distance clusters
temp.dist <- apply(as.matrix(my.acc[,c(20,19)], nrow=nrow(my.acc), ncol=2), 1, dist.match)

timediff = Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, 'units'), 'elapsed for spatial distance calcuation \n')

## Time comparison between each accident and all other points (will include self)
time.match <- function(pt){
  # difftime = time1 - time2
 temp.time <- difftime(pt, ct.data$pub_utc_timestamp, units="hours") 
which(temp.time < 1 & temp.time >= 0) # keep antecedents within 1hr
}

# time clusters
time.matches <- apply(as.data.frame(my.acc[,c("pub_utc_timestamp")]), 1, time.match)

timediff = Sys.time() - starttime
cat(round(timediff, 2), attr(timediff, 'units'), 'elapsed for time distance calcuation \n')

# intersect IDs from distance and time clusters
my.matches <- list()
for(i in 1:nrow(my.acc)){
my.matches[[i]] <- intersect(time.matches[[i]], temp.dist[[i]])
}

## keep IDs
save(my.matches, file = paste(localdir, "space.time.match.CT.Rdata", sep=""))
## Summarize my.matches
size.vector <- lapply(my.matches, length)
hist(as.numeric(size.vector))

# mini data frame of points in each space/time cluster
list.keep <- list()
for(i in 1:length(my.matches)){
  list.keep[[i]] <- ct.data[my.matches[[i]], ]
  }
save(list.keep, file = paste(localdir, "data.clusters.CT.Rdata", sep="")) 
  

### Reduce clusters to same "street"
list.keep.street <- list()
for(i in 1:length(list.keep)){
  list.keep.street[[i]] <- list.keep[[i]][which(list.keep[[i]]["street"]==my.acc$street[i]),]
  # identify root accident for cluster
  list.keep.street[[i]]["cluster.root"] <- ifelse(as.data.frame(list.keep.street[[i]]["alert_uuid"]) == my.acc$alert_uuid[i], 1, 0)
}
streets.size <- lapply(list.keep.street, nrow)
save(list = c("list.keep.street","streets.size"), file = paste(localdir, "street.clusters.CT.Rdata", sep=""))


## Identify alerts after accidents

## Time window for one hour after accidents in my.acc
time.match.post <- function(pt){
  # difftime = time1 - time2
  temp.time <- difftime(pt, ct.data$pub_utc_timestamp,units="hours") 
  which(temp.time > -1 & temp.time <= 0) # alerts within 1hr of accident
}

# time clusters
time.matches.post <- apply(as.data.frame(my.acc[,c("pub_utc_timestamp")]), 1, time.match.post)

# intersect IDs from distance and time clusters
my.matches.post <- list()
for(i in 1:nrow(my.acc)){
  my.matches.post[[i]] <- intersect(time.matches.post[[i]], temp.dist[[i]])
}

## keep IDs
#save(my.matches.post, file = paste(localdir, "space.time.match.CT.post.Rdata", sep=""))
## Summarize my.matches
#size.vector <- lapply(my.matches, length)

list.keep.post <- list()
for(i in 1:length(my.matches.post)){
  list.keep.post[[i]] <- ct.data[my.matches.post[[i]], ]
}
#save(list.keep.post, file = paste(localdir, "data.clusters.CT.post.Rdata", sep="")) 


### Reduce clusters to same "street"
list.keep.street.post <- list()
for(i in 1:length(list.keep.post)){
  list.keep.street.post[[i]] <- list.keep.post[[i]][which(list.keep.post[[i]]["street"]==my.acc$street[i]),]
  # identify root accident for cluster
  list.keep.street.post[[i]]["cluster.root"] <- ifelse(as.data.frame(list.keep.street.post[[i]]["alert_uuid"]) == my.acc$alert_uuid[i], 1, 0)
}
streets.size <- lapply(list.keep.street.post, nrow)
#save(list.keep.street.post, file = paste(localdir, "street.clusters.CT.post.Rdata", sep=""))


#final.size.post <- lapply(final.list.post, nrow)
save(list.keep.street.post, file=paste(localdir, "final.clusters.CT.post.Rdata", sep=""))

# Save entire working environment in one RData file now, to make sure all necessary objects are preserved

save(list = ls(), 
     file = file.path(localdir, "CT_Event_Sequence_Data_Prep.RData")
     )

# Save to S3
# Amend this when figure out all the objects to keep
# to add: ct.data, my.acc


zipname = paste0('CT_Event_Sequence_Data_Prep_', Sys.Date(), '.zip')

system(paste('zip -j', file.path('~/workingdata', zipname),
             file.path(localdir, "CT_Event_Sequence_Data_Prep.RData")
             # file.path(localdir, "space.time.match.CT.Rdata"),
             # file.path(localdir, "data.clusters.CT.Rdata"),
             # file.path(localdir, "street.clusters.CT.Rdata"),
             # file.path(localdir, "CT_Event_Sequence.RData")
             )
)


system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'SpecialEvents', zipname)
))

} else { # End prep new if statement

  zipname = dir(localdir)[grep("CT_Event_Sequence_Data_Prep_", dir(localdir))]
  
  if(length(zipname)== 0){
    zipname = "CT_Event_Sequence_Data_Prep_2018-10-23.zip"
    system(paste("aws s3 cp",
                 file.path(teambucket, 'SpecialEvents', zipname),
                 file.path('~', 'workingdata', zipname)))
  }
  
  system(paste('unzip -o', file.path(localdir, zipname), '-d',
         file.path(localdir, "SpecialEvents"))) # Unzip to SpecialEvents directory

  load(file.path(localdir, 'SpecialEvents', "CT_Event_Sequence_Data_Prep.RData"))
  # load(file.path(localdir, 'SpecialEvents', 'space.time.match.CT.Rdata'))
  # load(file.path(localdir, 'SpecialEvents', 'data.clusters.CT.Rdata'))
  # load(file.path(localdir, 'SpecialEvents', 'street.clusters.CT.Rdata'))
  # load(file.path(localdir, 'SpecialEvents', 'CT_Event_Sequence.RData'))
  
}

################################################################################################################

### Investigate major roads for singletons
major.roads <- c("I-95 N", "I-95 S", "I-84 E", "I-84 W", "I-91 S", "I-91 N", 
                 "Merrit Pkwy N", "Merrit Pkwy S", "Wilbur Cross Pkwy S", "Wilbur Cross Pkwy N",
                 "State Rte 8 N", "State Rte 8 S", "State Rte 9 N", "SR-2 W", "I-395 N",
                 "SR-2 E", "State Rte 9 S", "I-395 S", "I-684 N", "I-691 E", "I-691 W", 
                 "State Rte 15 S", "SR-8 S", "Berlin Tpke", "US-7 S", "State Rte 15 N", "US-7 N",
                 "I-384 W", "I-291 W")

# !!!! Ryan: I changed list.keep.str to list.keep.street !!!

for(dex in 1:length(list.keep.street)){
  list.keep.street[[dex]]$major.road <- (list.keep.street[[dex]]$street %in% major.roads)*1 
}



## plot number of antecedents for each accident
hist(as.numeric(streets.size) - 1, freq=F) # removes self


# 32% of accidents have no antecedents within 1 mi. and 1 hr. on same street

## build sublist of accidents without antecedents
lone.vec <- as.numeric(lapply(list.keep.street, function(x) nrow(x)))
## no antecedents
loner.list <- list.keep.street[which(lone.vec==1)]
# accidents with at least one antecedent
ants.list <- list.keep.street[which(lone.vec!=1)]
summary(as.numeric(lapply(ants.list, nrow))-1)

## consider frequency of accidents preceding accidents
length(loner.list) # 10385 singletons, .315
acc.antecedent <- as.numeric(lapply(ants.list, function(x) length(which(x$alert_type=="ACCIDENT"))))
length(which(acc.antecedent > 1)==TRUE) # 9192 antecedent accidents, .28

## compare accident times of singleton accidents to cluster observations
singleton.times <- as.vector(as.numeric(lapply(loner.list, function(x) hour(x$pub_utc_timestamp))))
## accidents that have at least one antecedent
ants.times <- as.vector(as.numeric(lapply(ants.list, function(x) hour(x$pub_utc_timestamp[which(x$cluster.root==1)]))))

## sample metrics.
## List.keep.street: list of all 32,953 accidents in these data. Each element of the list is a data frame for the alerts which occurred within one hour and 1 mile of the accident. The code below builds a data frame, where each row is the count of alerts for each accident. For other acciidents, subratct 1, to exclude the root accident.

metrics.frame <- data.frame(matrix(NA, nrow=length(list.keep.street), ncol=0))
metrics.frame$Jam <- as.numeric(lapply(list.keep.street, function(x) length(which(x$alert_type=="JAM"))))
metrics.frame$road.closed <- as.numeric(lapply(list.keep.street, function(x) length(which(x$alert_type=="ROAD_CLOSED"))))
metrics.frame$weather <- as.numeric(lapply(list.keep.street, function(x) length(which(x$alert_type=="WEATHERHAZARD"))))
metrics.frame$accident <- as.numeric(lapply(list.keep.street, function(x) length(which(x$alert_type=="ACCIDENT")) -1))
boxplot(metrics.frame)

# Better plot attempt
library(ggplot2)
mf <- metrics.frame %>%
  gather(key = "Alert_Type", value = "Count")

mf$Alert_Type <- factor(mf$Alert_Type, labels = c("Accident", "Jam", "Road Closed", "Weather/Hazard"))

ggplot(mf, aes(x = Count, group = Alert_Type)) +
  geom_histogram(bins = 25) + facet_wrap(~Alert_Type)


gp1 <- ggplot(mf %>% filter(Alert_Type != 'Road Closed'), aes(x = Alert_Type, y = Count, fill = Alert_Type)) +
  geom_boxplot(na.rm=T) +
  ylab("Count of preceding alerts", size = 10) +
  xlab("Alert type") +
  ggtitle("Mean count of preceding alerts for CT crashes") 

mc <- mf %>% filter(Alert_Type != 'Road Closed') %>%
  group_by(Alert_Type) %>%
  summarize(mean = mean(Count))

gp2 <- gp1 + annotate("text",
               x = c(1,2,3), y = mc$mean, label = round(mc$mean,2),
               size = 2)  
  
pdf('testplot.pdf', width = 6, height = 3)
print(gp1)
print(gp2)
dev.off()


mf.no.zero=mf
mf.no.zero[mf.no.zero==0]=NA
ggplot(mf.no.zero, aes(x = Alert_Type, y = Count)) +
  geom_violin(na.rm=T, adjust = 3)


ggplot(mf.no.zero, aes(x = Alert_Type, y = Count)) +
  geom_boxplot(na.rm=T)

### Lattice histograms
par(mfrow=c(2,2))
hist(metrics.frame$Jam, col="gray", xlim=c(0,10), main="",xlab = "Jam alerts in cluster")
hist(metrics.frame$road.closed, col="gray", xlim=c(0,10), main="", xlab = "Road closure alerts in cluster")
hist(metrics.frame$weather, col="gray", xlim=c(0,10), breaks=seq(1:10),main="",xlab = "Weather alerts in cluster")
hist(metrics.frame$accident, col="gray", xlim=c(0,10), main="", xlab = "Accident alerts in cluster")

summary(metrics.frame$Jam)




# Same, but now splitting by preceding and following events. Slow loop right now. !!! This actually should not have any post at all, per Ryan
mf <- vector()

for(i in 1:length(list.keep.street)){
  x = list.keep.street[[i]]
  
  jam.pre = length(which(x$alert_type=="JAM" & as.numeric(x$pub_millis) < as.numeric(x[x$cluster.root==1,'pub_millis'])))
  road_closed.pre = length(which(x$alert_type=="ROAD_CLOSED" & as.numeric(x$pub_millis) < as.numeric(x[x$cluster.root==1,'pub_millis'])))
  weatherhazard.pre = length(which(x$alert_type=="WEATHERHAZARD" & as.numeric(x$pub_millis) < as.numeric(x[x$cluster.root==1,'pub_millis'])))
  acc.pre = length(which(x$alert_type=="ACCIDENT" & as.numeric(x$pub_millis) < as.numeric(x[x$cluster.root==1,'pub_millis'])))
  
  jam.post = length(which(x$alert_type=="JAM" & as.numeric(x$pub_millis) >= as.numeric(x[x$cluster.root==1,'pub_millis'])))
  road_closed.post = length(which(x$alert_type=="ROAD_CLOSED" & as.numeric(x$pub_millis) >= as.numeric(x[x$cluster.root==1,'pub_millis'])))
  weatherhazard.post = length(which(x$alert_type=="WEATHERHAZARD" & as.numeric(x$pub_millis) >= as.numeric(x[x$cluster.root==1,'pub_millis'])))
  acc.post = length(which(x$alert_type=="ACCIDENT" & as.numeric(x$pub_millis) >= as.numeric(x[x$cluster.root==1,'pub_millis']))) - 1
  
  mf <- rbind(mf, data.frame(jam.pre, road_closed.pre, weatherhazard.pre, acc.pre, 
             jam.post, road_closed.post, weatherhazard.post, acc.post))
  
  if(i %% 5000 == 0) cat(i, ". ")
}

m2 <- mf %>% 
  gather(key = "Alert_type_prepost", value = "Count")

m2$Pre = grep("pre", m2$Alert_type_prepost) == rownames(m2)

m2$Alert_type = unlist(lapply(strsplit(m2$Alert_type_prepost, "\\."), function(x) x[[1]]))
 
ggplot(m2, aes(x = Alert_type, y = Count, fill = Pre)) +
  geom_boxplot()

# Make another attempt, median count of events before or after
means <- data.frame(mean = apply(mf, 2, mean))
means$ses <- apply(mf, 2, function(x) sd(x)/sqrt(length(x)-1))

means$Alert_type = unlist(lapply(strsplit(rownames(means), "\\."), function(x) x[[1]]))
means$Alert_type <- factor(means$Alert_type, labels = c("Accident", "Jam", "Road Closed", "Weather/Hazard"))

means$Pre = unlist(lapply(strsplit(rownames(means), "\\."), function(x) x[[2]]))
means$Pre <- factor(means$Pre, labels = c("2Post", "1Pre"))
means$Pre <- as.factor(as.character(means$Pre))
levels(means$Pre) = c("Pre", "Post")

ggplot(means %>% filter(Alert_type != "Road Closed"), aes(x = Alert_type, y = mean, fill = Pre)) +
  geom_bar(stat='identity',
           position=position_dodge()) +
  ylab("Mean count of related alerts") +
  xlab("Alert type") +
  ggtitle("Mean count of related alerts for CT crashes") +
  geom_text(aes(label = round(mean, 2)),
            position = position_dodge(0.9),
            vjust = -1) +
  guides(fill= guide_legend(title="Pre/post crash"))

## Need comparison to total CT events
summary(as.factor(ct.data$alert_type))/nrow(ct.data)

## Are singleton accidents on major roads? only 25% on major roads
singleton.roads <- as.numeric(lapply(loner.list, function(x) length(which(x$major.road==1))))
## Compared to accidents with antecedents, 68% on major roads
cluster.roads <- as.numeric(lapply(ants.list, function(x) length(which(x$major.road[which(x$cluster.root==1)]==1))))

## drill down into alert sub types
# build groups - not fully disjoint

## road conditions/access
road.hazards <- c("HAZARD_ON_ROAD", "HAZARD_ON_ROAD_CAR_STOPPED", "HAZARD_ON_SHOULDER_ANIMALS")

# visibility limitations
weather.vis <- c("HAZARD_WEATHER_FOG", "HAZARD_WEATHER_HEAVY_RAIN", "HAZARD_WEATHER_HEAVY_SNOW",
                 "HAZARD_WEATHER_MONSSON")

# degredation of road surface - ice, snow, floods
weather.surf <- c("HAZARD_WEATHER_FREEZING_RAIN", "HAZARD_WEATHER_HEAVY_SNOW",
                  "HAZARD_WEATHER_HEAVY_RAIN")

### Build relative distance and time from each record to cluster root
for(dex in 1:length(list.keep.street)){
  # dex <- 1
  list.keep.street[[dex]]$root.dist <- spDistsN1(as.matrix(list.keep.street[[dex]][,c(20,19)]), 
                              as.matrix(list.keep.street[[dex]][which(list.keep.street[[dex]]$cluster.root==1), c(20,19)]), 
                              longlat=TRUE)*0.6213712
  
  ## Build time from cluster root (in minutes)
  list.keep.street[[dex]]$root.time <- difftime(list.keep.street[[dex]]$pub_utc_timestamp[which(list.keep.street[[dex]]$cluster.root==1)],
                                                list.keep.street[[dex]]$pub_utc_timestamp, units="mins")
  
} # end for loop that builds relative distances (space, time) in each cluster







## no followers
## build sublist of accidents without antecedents
lone.vec.post <- as.numeric(lapply(list.keep.street.post, function(x) nrow(x)))
loner.list.post <- list.keep.street.post[which(lone.vec.post==1)]
# accidents with at least one antecedent
follow.list <- list.keep.street.post[which(lone.vec.post!=1)]
summary(as.numeric(lapply(follow.list, nrow))-1)

### Summarize frequencies
length(list.keep.post)

# ! missing 'final.list' and 'final.list.post'

## after accidents
hist(as.numeric(lapply(final.list.post, nrow)))

## before accidents (antecedent events)
hist(as.numeric(lapply(final.list, nrow)))

## Pretty similar, before and after (~Poisson distrubution)



########################################################################################################
## Scraps from other code for reference

SCRATCH  = F

if(SCRATCH){
    # Copy to S3
    system(paste("aws s3 cp",
                 file.path(output.loc, fn),
                 file.path(teambucket, i, fn)))
    
  

  # Set up cluster. 
  avail.cores <- parallel::detectCores()
  if(avail.cores > length(yearmonths)) avail.cores = length(yearmonths) # use only cores necessary
  cl <- makeCluster(avail.cores) 
  registerDoParallel(cl)
  
}

