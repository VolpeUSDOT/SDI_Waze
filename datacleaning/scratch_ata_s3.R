# Read-write from local machine to ATA S3

library(aws.s3)

# set up AWS Access Keys in the console, then save the plain text file of the key and secret in a secure location.
# in Terminal, use `aws configure` and enter those values by pasting into the console. Then use the following to tell R to look for the credentials in that location (.aws directory in the  home folder)

aws.signature::use_credentials()


# local csv to Rdata frame; pick a csv on the local machine
test <- readr::read_csv("Summary_RIEM_Regression_Output.csv")

# save frame to s3
s3save(test, object = "working/test.Rdata", bucket = "ata-waze")

# read frame from bucket
testR <- s3load("working/test.Rdata", bucket = "ata-waze")

# write frame to bucket
s3save(testR, object = "working/testOut.Rdata", bucket = "ata-waze")

