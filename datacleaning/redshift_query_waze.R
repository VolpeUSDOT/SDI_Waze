# Test Redshift queries and close connection



dbGetQuery(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

# For FARS, will need to edit
dbGetQuery(conn, "SELECT day_week, sum(drunk_dr) from fars_accident_csv group by day_week order by 2 desc")

dbDisconnect(conn)
