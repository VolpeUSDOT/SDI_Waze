# Test of SDC Redshift connection, following John Alesse's notes for pulling data from Redshift
# additional detail from https://aws.amazon.com/blogs/big-data/connecting-r-with-amazon-redshift/
# https://daniel-workspace.securedatacommons.com:8888
# install.packages("RJDBC", dep = T)
library(RJDBC)

# Look in .redshiftTools. If empty, download the driver from Amazon.
# Download to a hidden folder for cleaner workspace. Latest in 2018-02 is 1.2.10.1009.
# make sure to use mode = "wb" on Windows OS: "Since Windows (unlike Unix-alikes) does distinguish between text and binary files, care is needed that other binary file types are transferred with mode = "wb"."

if(length(dir("~/.redshiftTools"))==0){
    system('mkdir ~/.redshiftTools')
    download.file('https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC42-1.2.10.1009.jar', '~/.redshiftTools/RedshiftJDBC42-1.2.10.1009.jar', mode = "wb")
}

driver <- JDBC(driverClass = "com.amazon.redshift.jdbc42.Driver", 
               classPath = "~/.redshiftTools/RedshiftJDBC42-1.2.10.1009.jar",
               identifier.quote="`")

# .jclassLoader()$setDebug(1L) # Set this to get more details if recieving 'class not found' error. Same error on ec2 instance inside SDC until changing download mode.

# Specify username and password manually, once:
if(Sys.getenv("sdc_waze_username")==""){
  cat("Please enter SDC Waze username and password manually the first time accessing the Redshift database, using: \n Sys.setenv('sdc_waze_username' = <see email from SDC Administrator>) \n Sys.setenv('sdc_waze_password' = <see email from SDC Administrator>)")
  }

url <- sprintf("jdbc:redshift://prod-dot-sdc-redshift-cluster.cctxatvt4w6t.us-east-1.redshift.amazonaws.com:5439/dot_sdc_redshift_db?user=%s&password=%s", 
               Sys.getenv("atadev_username"), Sys.getenv("atadev_password"))

conn <- dbConnect(driver, url)
