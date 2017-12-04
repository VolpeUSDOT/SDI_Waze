library(jsonlite)
allmonths <- c("04", "05", "06", "07", "08", "09", "10")

for (month in allmonths){
  setwd(paste("F:/USER/PCOMMON/Safety Team/Data Initiative 2017/Waze Pilot/Waze Data Pull/Waze Data Pull 111317/waze/", month, "_2017/", sep = ""))
  filenames <- list.files()
  path <- paste("F:/USER/PCOMMON/Safety Team/Data Initiative 2017/Waze Pilot/Waze Data Pull/Waze Data Pull 111317/waze/", month, "_2017/", sep = "")
  i=1
  z=1
  xtemp <- stream_in(file(paste(path, filenames[i], sep = "")))
  xtemp2 <- flatten(xtemp)
  xtemp2$filename <- filenames[i]
  while((i+z)<= length(filenames)){
    x <- substr(filenames[i], 1,14)
    y <- substr(filenames[i+z], 1,14)
    infox <- file.info(paste(path, filenames[i], sep = ""))
    infoy <- file.info(paste(path, filenames[i+z], sep = ""))
    if (infoy$size == 0){
      z = z+1
    }else if(y == x){
      ytemp <- stream_in(file(paste(path, filenames[i+z], sep = "")))
      ytemp2 <- flatten(ytemp)
      ytemp2$filename <- filenames[i+z]
      xtemp2 <- rbind(xtemp2, ytemp2)
      z = z+1 
    }
    else{
      write.csv(xtemp2, file = paste(x, ".csv", sep = ""))
      xtemp <- stream_in(file(paste(path, filenames[i+z], sep = "")))
      xtemp2 <- flatten(xtemp)
      xtemp2$filename <- filenames[i+z]
      i=i+z
      z=1
    }
  }
  write.csv(xtemp2, file = paste(x, ".csv", sep = ""))
}