# scratch for parallelization
d.all = d
d = d[1:500,]

accfile = d
incfile = d
acctimevar = "time"
inctimevar1 = "time"
inctimevar2 = "last.pull.time"
accidvar = "uuid"
incidvar = "uuid"


# install.packages(c("foreach", "doParallel"), dep = T)
library(foreach) 
library(doParallel) # includes iterators and parallel

cl <- makeCluster(parallel::detectCores()) # make a cluster of all available cores
registerDoParallel(cl)


nonpar <- system.time(foreach(i=1:1000) %do% sqrt(i))
parvers <- system.time(foreach(i=1:1000) %dopar% sqrt(i)) # not actually faster, because overhead of scheduling tiny task is greater than the task itself (from Vignette). But the machinery works.

starttime <- Sys.time()
writeLines("", "waze_waze_log.txt") # to store messages
sink("waze_waze_log.txt", append=TRUE)

linktable <- foreach(i=1:nrow(accfile), .combine = rbind) %dopar% {
  
    ei = accfile[i,]
    dist.i <- spDists(ei, incfile, longlat = T)*0.6213712 # spDists gives units in km, convert to miles
    dist.i.5 <- which(dist.i <= 0.5)
  
    # Spatially matching
    d.sp <- incfile[dist.i.5,]

    dist.i <- spDists(ei, incfile, longlat = T)*0.6213712 # spDists gives units in km, convert to miles
    dist.i.5 <- which(dist.i <= 0.5)
    
    # Temporally matching
    # Match between the first reported time and last pull time of the Waze event. Last pull time is after the earliest time of EDT, and first reported time is earlier than the latest time of EDT
    if(class(ei)=="SpatialPointsDataFrame") { ei <- as.data.frame(ei) }
    if(class(d.sp)=="SpatialPointsDataFrame") { d.sp <- as.data.frame(d.sp) }
    
    
    d.t <- d.sp[d.sp[,inctimevar2] >= ei[,acctimevar]-60*60 & d.sp[,inctimevar1] <= ei[,acctimevar]+60*60,] 
    
    id.accident <- rep(as.character(ei[,accidvar]), nrow(d.t))
    id.incidents <- as.character(d.t[,incidvar])
    
    if(i %% 100 == 0) {
      timediff <- round(Sys.time()-starttime, 2)
      cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n")
      cat("approx", timediff/i * (nrow(accfile)-i), attr(timediff, "units"), "remaining \n")
      cat(rep("<>",20), "\n\n")
      }
    
    data.frame(id.accident, id.incidents) # rbind this output
    }
# sink()

    
timediff <- round(Sys.time()-starttime, 2)
cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")

stopCluster(cl) # need to stop the cluster.


