# Test of SDC Redshift connection, now using Postgre/SQL which does not rely on Java

# additional detail from https://aws.amazon.com/blogs/big-data/connecting-r-with-amazon-redshift/
# https://daniel-workspace.securedatacommons.com:8888
# install.packages("RPostgreSQL", dep = T)
library(RPostgreSQL)

# Specify username and password manually, once:
if(Sys.getenv("sdc_waze_username")==""){
  cat("Please enter SDC Waze username and password manually the first time accessing the Redshift database, using: \n Sys.setenv('sdc_waze_username' = <see email from SDC Administrator>) \n Sys.setenv('sdc_waze_password' = <see email from SDC Administrator>)")
}


redshift_host <- "prod-dot-sdc-redshift-cluster.cctxatvt4w6t.us-east-1.redshift.amazonaws.com"
redshift_port <- "5439"
redshift_user <- Sys.getenv("sdc_waze_username")
redshift_password <- Sys.getenv("sdc_waze_password")
redshift_db <- "dot_sdc_redshift_db"

drv <- dbDriver("PostgreSQL")
conn <- dbConnect(
  drv,
  host=redshift_host,
  port=redshift_port,
  user=redshift_user,
  password=redshift_password,
  dbname=redshift_db)

tryCatch({
  example_query <- "SELECT * FROM my_table LIMIT 5"
  results <- dbGetQuery(conn, example_query)
  print(results)
}, finally = {
  dbDisconnect(conn)
})



