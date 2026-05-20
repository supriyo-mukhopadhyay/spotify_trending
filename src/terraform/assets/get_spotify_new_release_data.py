from typing import Callable, Dict, Any
import logging
import requests
import sys
import json
import os
from dotenv import load_dotenv
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import time
import pandas as pd
import boto3
from typing import Dict

load_dotenv()
# Get arguments that are passed when running the script
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",
        "target_path",
        "source_path"
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
# Spark Session, the entry point to programming Spark with the Dataset and DataFrame API
spark = glueContext.spark_session

job = Job(glueContext)
# Initialize the Job
job.init(args["JOB_NAME"], args)


CLIENT_ID = "8d1b1e30ecea43daa9f4d3bfc41c9414"
CLIENT_SECRET = "32c4b4532c17462face29d83c456ff32"

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = "eu-west-1"
BUCKET_NAME = "spapi-808429836131-eu-west-1-bucket"
URL_TOKEN = "https://accounts.spotify.com/api/token"
URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"
URL_ALBUM_TRACKS = "https://api.spotify.com/v1/albums"


################################# Logging ###############################################
# All application logs are saved in producer.log file in project directory
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("spotify_staging.log"),
        logging.StreamHandler(sys.stdout),
    ],
)


class authentication:

    def __init__(self):
        logging.info({"Message": "Authentication class initiated !"})

    def get_token(self, client_id: str, client_secret: str, url: str) -> Dict[Any, Any]:
        """Allows to perform a POST request to obtain an access token

        Args:
            client_id (str): App client id
            client_secret (str): App client secret
            url (str): URL to perform the post request

        Returns:
            Dict[Any, Any]: Dictionary containing the access token
        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        try:
            response = requests.post(url=url, headers=headers, data=payload)
            response_json = json.loads(response.content)

            return response_json

        except Exception as err:
            logging.error(
                {
                    "Message": f"error getting authentication token : {err}",
                    "line": format(
                        sys.exc_info()[
                            -1
                        ].tb_lineno  # pyright: ignore[reportOptionalMemberAccess]
                    ),
                }
            )
            return {}

    def get_auth_header(self, access_token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {access_token}"}


class endpoints:

    def __init__(self):
        logging.info({"Message": "endpoints class initiated !"})
        self.authentication = authentication()

    def get_paginated_new_releases(
        self, base_url: str, access_token: str, get_token: Callable, **kwargs
    ) -> list:
        """Performs paginated calls to the new releases endpoint. Manages token refresh when required.

        Args:
            base_url (str): Base URL for API requests
            access_token (str): Access token
            get_token (Callable): Function that requests access token

        Returns:
            list: Request responses stored as a list
        """
        headers = self.authentication.get_auth_header(access_token=access_token)
        request_url = base_url
        new_releases_data = []

        try:
            while request_url:
                logging.info(
                    {
                        "Message": f"Requesting to: {request_url} -> return paginated new releases"
                    }
                )
                response = requests.get(url=request_url, headers=headers)
                retry_after = response.headers.get("Retry-After")

                if response.status_code == 401:
                    token_response = get_token(**kwargs)
                    headers = self.authentication.get_auth_header(
                        access_token=token_response["access_token"]
                    )
                    response = requests.get(url=request_url, headers=headers)
                # print(response)

                if response.status_code == 429:
                    logging.info(
                        {"Message": f"waiting for {retry_after} untill next request"}
                    )
                    time.sleep(int(retry_after))

                if response.status_code == 200:
                    response_json = response.json()
                    new_releases_data.extend(response_json["albums"]["items"])
                    request_url = response_json["albums"]["next"]
            return new_releases_data

        except Exception as err:
            logging.error(
                {
                    "Message": f"Error occurred during request {err} for request url {request_url}",
                    "line": format(
                        sys.exc_info()[
                            -1
                        ].tb_lineno  # pyright: ignore[reportOptionalMemberAccess]
                    ),
                }
            )
            return []

    def get_paginated_album_tracks(
        self,
        base_url: str,
        access_token: str,
        album_id: str,
        get_token: Callable,
        **kwargs,
    ) -> list:
        """Performs paginated requests to the album/{album_id}/tracks endpoint

        Args:
            base_url (str): Base URL for endpoint requests
            access_token (str): Access token
            album_id (str): Id of the album to be queried
            get_token (Callable): Function that requests access token

        Returns:
            list: Request responses stored as a list
        """

        headers = self.authentication.get_auth_header(access_token=access_token)
        #  Create the requests_url by using the base_url and album_id parameters. At the end, you will add tracks to the URL endpoint.
        request_url = f"{base_url}/{album_id}/tracks"
        album_data = []

        try:
            while request_url:
                logging.info(
                    {
                        "Message": f"Requesting to: {request_url} -> return paginated album details"
                    }
                )
                # Perform a GET request using the request_url and headers that you created in the previous steps.
                response = requests.get(url=request_url, headers=headers)
                retry_after = response.headers.get("Retry-After")
                # print(f"response {response}")

                if response.status_code == 401:  # Unauthorized
                    # Handle token expiration and update.
                    token_response = get_token(**kwargs)
                    # Call get_auth_header() function with the "access_token" from the token_response.
                    headers = self.authentication.get_auth_header(
                        access_token=token_response["access_token"]
                    )
                    response = requests.get(url=request_url, headers=headers)
                    logging.info(
                        {
                            "Message": "Token has been refreshed for paginated album details"
                        }
                    )

                if response.status_code == 429:
                    logging.info(
                        {"Message": f"waiting for {retry_after} untill next request"}
                    )
                    time.sleep(int(retry_after))

                if response.status_code == 200:
                    # Convert the response to json using the json() method.
                    # print(response.headers.get("Retry-After"))
                    response_json = response.json()
                    # print(response_json)
                    album_data.extend(response_json["items"])
                    request_url = response_json["next"]
            return album_data

        except Exception as err:
            logging.error(
                {
                    "Message": f"Error occurred during request {err} for request url {request_url}",
                    "line": format(
                        sys.exc_info()[
                            -1
                        ].tb_lineno  # pyright: ignore[reportOptionalMemberAccess]
                    ),
                }
            )
            return []


# main()function

kwargs = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "url": URL_TOKEN,
}

auth = authentication()
ep = endpoints()
token = auth.get_token(**kwargs)

new_releases = ep.get_paginated_new_releases(
    base_url=URL_NEW_RELEASES,
    access_token=token["access_token"],
    get_token=auth.get_token,
    **kwargs,
)


albums_ids = [album["id"] for album in new_releases]

album_items = []

def transform_data(album_data):
    
    album_item = {}
    for x in album_data:
        artist_external_urls="" 
        artist_href = ""
        artist_id = ""
        artist_name = ""
        artist_type = ""
        artist_uri = ""
        available_markets = ""
        
        album_item["album_id"] = album_id
        album_item["number_of_artist"] = len(x["artists"])
        for y in x["artists"]:
            artist_external_urls = artist_external_urls + y["external_urls"]["spotify"] +","
            artist_href = artist_href + y["href"] + ","
            artist_id = artist_id + y["id"] + ","
            artist_name = artist_name + y["name"] + ","
            artist_type = artist_type + y["type"] + ","
            artist_uri = artist_uri + y["uri"] + ","
        album_item["artist_external_urls"] = artist_external_urls
        album_item["artist_href"] = artist_href
        album_item["artist_id"] = artist_id
        album_item["artist_name"] = artist_name
        album_item["artist_type"] = artist_type
        album_item["artist_uri"] = artist_uri
        album_item["available_markets"] = len(x["available_markets"])
        for y in x["available_markets"]:
            available_markets = available_markets + y + ","
        album_item["available_markets"] = available_markets
        album_item["disc_number"] = x["disc_number"]
        album_item["duration_ms"] = x["duration_ms"]
        album_item["explicit"] = x["explicit"]
        album_item["external_urls"] = x["external_urls"]
        album_item["href"] = x["href"]
        album_item["id"] = x["id"]
        album_item["name"] = x["name"]
        album_item["preview_url"] = str(x["preview_url"])
        album_item["track_number"] = x["track_number"]
        album_item["type"] = x["type"]
        album_item["uri"] = x["uri"]
        album_item["is_local"] = x["is_local"]  
        
    return album_item

for album_id in albums_ids:

    album_data = ep.get_paginated_album_tracks(
        base_url=URL_ALBUM_TRACKS,
        access_token=token["access_token"],
        album_id=album_id,
        get_token=auth.get_token,
        **kwargs,
    )
     
    album_item = transform_data(album_data)
    album_items.append(album_item)  


session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=REGION,
)

# Get arguments that are passed when running the script
s3Resource = boto3.resource(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY
)
s3Client = boto3.client(
    "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY
)

try:
    print()
    with open("./myfile.json", "w+") as file:
        file.write(json.dumps(album_items))

    s3Client.put_object(
                Body=json.dumps(album_items), Bucket=BUCKET_NAME, Key="staging_data/json"
            )
except Exception as e:
    logging.error(
                {
                    "Message": f"Error occurred during uploading staging json data {e} ",
                    "line": format(
                        sys.exc_info()[
                            -1
                        ].tb_lineno  # pyright: ignore[reportOptionalMemberAccess]
                    ),
                }
            )

# df = pd.DataFrame(album_items)
# print(df.head)

try:
    #Create pandas data frame
    source_pd = pd.DataFrame(album_items)
    logging.info({
        "Message": f"Curated data pandas df: {source_pd.head()}"
    })
    
    # Generate a Spark DataFrame from Pandas DataFrame
    dest_pd = spark.createDataFrame(source_pd)
    
    # Generate a  Glue DynamicFrame from Spark DataFrame
    dest_df = DynamicFrame.fromDF(dest_pd, glueContext, "dest_df")
    
    # Instanciate a Glue Job with the Glue context
    connection_options = {
        "path": f"s3://{args['s3_bucket']}/{args['target_path']}/",
    }

    # Write the previous Glue DynamicFrame to a parquet file in a given S3 path
    datasink = glueContext.write_dynamic_frame.from_options(
        frame=dest_df,
        connection_type="s3",
        format="glueparquet",
        connection_options=connection_options,
        transformation_ctx="datasink",
    )
    
    
except Exception as err:
    logging.error(
                {
                    "Message": f"Error occurred during uploading curated data {err} ",
                    "line": format(
                        sys.exc_info()[
                            -1
                        ].tb_lineno  # pyright: ignore[reportOptionalMemberAccess]
                    ),
                }
            )

job.commit()