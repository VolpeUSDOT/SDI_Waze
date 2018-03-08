#!/bin/bash

# Shell script to move files from /home/daniel/ to the SDI team S3 bucket. Track the time, and then list the bucket contents.

# From Jupyter Notebook terminal, execute this script as follows:

# sudo /home/daniel/SDI_Waze/utility/sdc_s3_out.sh

# If that fails, make sure permissions are set correctly, so the root user can execute this script.

# sudo chmod u+x /home/daniel/SDI_Waze/utility/sdc_s3_out.sh

STARTTIME=$(date +%s)

sudo aws s3 cp /home/daniel/tempout s3://prod-sdc-sdi-911061262852-us-east-1-bucket --recursive --include "*"

ENDTIME=$(date +%s)

echo "$(($ENDTIME - $STARTTIME)) second(s) elapsed"

sudo aws s3 ls s3://prod-sdc-sdi-911061262852-us-east-1-bucket

echo "Delete files in temporary output directory?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) rm -rf /home/daniel/tempout/*; break;;
        No ) exit;;
    esac
done
