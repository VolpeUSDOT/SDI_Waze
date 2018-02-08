# Test of SDC Redshift connection, following John Alesse's notes for pulling data from Redshift

# install.packages("RJDBC", dep = T)
library(RJDBC)

# Need to do this onely once. Download to a hidden folder for cleaner workspace. Latest in 2018-02 is 1.2.10.1009.

if(length(dir("~/.redshiftTools"))==0){
    system('mkdir ~/.redshiftTools')
    download.file('https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC42-1.2.10.1009.jar', '~/.redshiftTools/RedshiftJDBC42-1.2.10.1009.jar')
}

driver <- JDBC(driverClass = "com.amazon.redshift.jdbc42.Driver", 
               classPath = "~/.redshiftTools/RedshiftJDBC42-1.2.10.1009.jar", 
               identifier.quote="`")

# Specify username and password manually, once:
if(Sys.getenv("sdc_waze_username")==""){
  cat("Please enter SDC Waze username and password manually the first time accessing the Redshift database, using: \n Sys.setenv('sdc_waze_username' = <see email from SDC Administrator>) \n Sys.setenv('sdc_waze_password' = <see email from SDC Administrator>)")
  }

url <- sprintf("jdbc:redshift://prod-dot-sdc-redshift-cluster.cctxatvt4w6t.us-east-1.redshift.amazonaws.com:5439/dot_sdc_redshift_db?user=%s&password=%s", 
               Sys.getenv("atadev_username"), Sys.getenv("atadev_password"))

conn <- dbConnect(driver, url)


dbGetQuery(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

# For FARS, will need to edit
dbGetQuery(conn, "SELECT day_week, sum(drunk_dr) from fars_accident_csv group by day_week order by 2 desc")

dbDisconnect(conn)
