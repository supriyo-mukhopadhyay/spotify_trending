import awswrangler as wr
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import boto3
import logging
import sys

load_dotenv()

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
# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = "eu-west-1"
BUCKET_NAME = "spapi-808429836131-eu-west-1-bucket"
DATABASE_NAME = "spapi-processed-data"

# Create AWS session with credentials. using boto3 lib
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=REGION,
)

def create_catalog_db():
    databases = wr.catalog.databases(boto3_session=session)
    
    database_list_objs = databases.values.tolist()
    database_name = []
    for x in range(len(database_list_objs)):
        database_name.append(database_list_objs[x][0])

    if DATABASE_NAME not in database_name:
        wr.catalog.create_database(DATABASE_NAME, boto3_session=session)
        logging.info(
            {
                "Message": f"created database {DATABASE_NAME} ",
            }
        )
    else:
        logging.info(
            {
                "Message": f"database {DATABASE_NAME} already exists ",
            }
        )

try:
    glue_client = boto3.client(
        "glue",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )
except Exception as e:
    logging.error(
        {
            "Message": f"boto3 glue client creation error: {e}",
        }
    )
        

def create_crawler():
    try:
        response = glue_client.create_crawler(
            Name="spotify-api-process-data-crawler",
            Role="spotify_api-glue-role",
            DatabaseName=DATABASE_NAME,
            Description="spapi spotify data",
            Targets={
                "S3Targets": [
                    {
                        "Path": f"s3://{BUCKET_NAME}/processed_data/",
                    },
                ]
            },
        )
        
        return response
    except Exception as e:
        logging.error(
            {
                "Message": f"boto3 glue client create crawler error: {e}",
            }
        )
        
def start_crawler():
    try:
        
        response = glue_client.list_crawlers()
        crawler_name = response["CrawlerNames"][0]
        print(crawler_name)
    except Exception as e:
        print(e)
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        print(response)
    except Exception as e:
        print(e)
        
def create_athena_system(): 
    try:
        table_dataframe = wr.catalog.tables(database=DATABASE_NAME, boto3_session=session)
        print(table_dataframe["Database"])
    except Exception as e:
        print(e)
    try:
        sql = "SELECT * FROM processed_data"
        df = wr.athena.read_sql_query(
            sql,
            database=DATABASE_NAME,
            s3_output=f"s3://{BUCKET_NAME}/athena_output/",
            boto3_session=session,
        )
        print(df.head())
    except Exception as e:
        print(e)

# create_crawler()
# start_crawler()
create_athena_system()