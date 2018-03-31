# Read-write from local machine to ATA S3

library(aws.s3)

# set up AWS Access Keys in the console, then save the plain text file of the key and secret in a secure location.
# in Terminal, use `aws configure` and enter those values by pasting into the console. Then use the following to tell R to look for the credentials in that location (.aws directory in the  home folder)

aws.signature::use_credentials()


# local csv to Rdata frame; pick a csv on the local machine
test <- data.frame(runif(1000))

# save frame to s3
s3save(test, object = "working/test.Rdata", bucket = "ata-waze")
rm(test)

# read frame from bucket
s3load("working/test.Rdata", bucket = "ata-waze")
head(test)

# Moving files up ----
# From local machine, use SFTP to transfer census, hex, and derived data to the analysis instance, in "s3_transfer".
# Then run this to move all files over directly
system("aws s3 cp /home/dflynn-volpe/s3_transfer s3://ata-waze --recursive --include '*'")

