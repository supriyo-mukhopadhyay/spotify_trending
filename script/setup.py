import boto3
from botocore.exceptions import ClientError
import logging
import sys

from dotenv import load_dotenv
import os

################################# Logging ###############################################
# All application logs are saved in producer.log file in project directory
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

load_dotenv()


# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = "eu-west-1"
BUCKET_NAME_STAGING_DATA = "spapi-808429836131-eu-west-1-bucket"
# BUCKET_NAME_PROCESSED_DATA = "ep011-808429836131-eu-north-1-processed-bucket"
# BUCKET_NAME_STAGING_SCRIPTS = "ep011-808429836131-eu-north-1-staging-scripts"

# Create AWS session with credentials. using boto3 lib
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRETE_ACCESS_KEY,
    region_name=REGION,
)

# Get arguments that are passed when running the script
s3Resource = boto3.resource(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)
s3Client = boto3.client(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_ACCESS_KEY
)


def bucketNameValidation(BucketName):
    val = 0
    try:
        for (
            bucket
        ) in s3Resource.buckets.all():  # pyright: ignore[reportAttributeAccessIssue]
            # s3BucketList.append(bucket.name)
            # print(bucket)
            if BucketName == bucket.name:
                val = 1
                break
            else:
                continue
        return val
    except Exception as e:
        logging.error(e)


def createBucket(bucketName, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    """
    BucketName = bucketName
    # Create bucket
    try:
        if bucketNameValidation(BucketName) == 0:
            if region is None:
                s3Client.create_bucket(Bucket=BucketName)
            else:
                location = {"LocationConstraint": region}
                s3Client.create_bucket(
                    Bucket=BucketName, CreateBucketConfiguration=location
                )
                logging.info("Bucket created !!!")
        else:
            logging.info("Bucket exists !!!")
    except ClientError as e:
        logging.error(format(sys.exc_info()[-1].tb_lineno))  # type: ignore
        logging.error(e)
        return False
    return True


createBucket(BUCKET_NAME_STAGING_DATA, REGION)
# createBucket(BUCKET_NAME_STAGING_SCRIPTS, REGION)
# createBucket(BUCKET_NAME_PROCESSED_DATA, REGION)


#aws s3 cp .\src\terraform\assets\get_spotify_new_release_data.py s3://spapi-808429836131-eu-west-1-bucket/staging_scripts/get_spotify_new_release_data.py
#aws glue start-job-run --job-name spotify_api-spotify-staging-job