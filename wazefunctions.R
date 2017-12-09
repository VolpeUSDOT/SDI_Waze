# Functions for working with Waze and EDT data

# Make a link table to match events
# Location in EDT is in GPSLong_New and GPS_Lat, time is in CrashDate_Local.
# Location in Waze is in lon and lat, time is in time.

makelink <- function(edtfile, wazefile){
  
  linktable <- vector()
  starttime <- Sys.time()
  
  for(i in 1:nrow(edtfile)){ # i = 1
    ei = edtfile[i,]
    dist.i <- spDists(ei, wazefile, longlat = T)*0.6213712 # spDists gives units in km, convert to miles
    dist.i.5 <- which(dist.i <= 0.5)
    
    # Spatially matching
    d.sp <- wazefile[dist.i.5,]
    
    # Temporally matching
    d.t <- d.sp[d.sp$time > ei$CrashDate_Local-60*60 & d.sp$time <= ei$CrashDate_Local+60*60,] 
    
    id.edt <- rep(as.character(ei$ID), nrow(d.t))
    uuid.waze <- as.character(d.t$uuid)
    
    linktable <- rbind(linktable, data.frame(id.edt, uuid.waze))
    
    if(i %% 100 == 0) {
      timediff <- round(Sys.time()-starttime, 2)
      cat(i, "complete \n", timediff, attr(timediff, "units"), "elapsed \n", rep("<>",20), "\n")
    }
  } # end loop
  
  linktable
}


