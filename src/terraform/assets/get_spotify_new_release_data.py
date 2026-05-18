from typing import Callable, Dict, Any
import logging
import requests
import sys
import json
import os
from dotenv import load_dotenv
from awsglue.context import GlueContext
from pyspark.core.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


load_dotenv()
# Get arguments that are passed when running the script
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",
        "target_path",
    ],
)


CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

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
        logging.info({"Message":"Authentication class initiated !"})
    
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
            }
        )
            return {}
    
    def get_auth_header(self, access_token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {access_token}"}
    


class endpoints:
    
    def __init__(self):
        logging.info({"Message":"endpoints class initiated !"})
        self.authentication = authentication()
    
    def get_paginated_new_releases(self, 
        base_url: str, access_token: str, get_token: Callable, **kwargs
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
                logging.info({"Message":f"Requesting to: {request_url} -> return paginated new releases"})
                response = requests.get(url=request_url, headers=headers)

                if response.status_code == 401:
                    token_response = get_token(**kwargs)
                    headers = self.authentication.get_auth_header(access_token=token_response["access_token"])
                # print(response)
                
                response_json = response.json()
                new_releases_data.extend(response_json["albums"]["items"])
                request_url = response_json["albums"]["next"]
            return new_releases_data

        except Exception as err:
            logging.error(
            {
                "Message": f"Error occurred during request {err} for request url {request_url}",
            })
            return []
    
    
    def get_paginated_album_tracks(self,
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
                logging.info({"Message": f"Requesting to: {request_url} -> return paginated album details"})
                # Perform a GET request using the request_url and headers that you created in the previous steps.
                response = requests.get(url=request_url, headers=headers)
                # print(f"response {response}")

                if response.status_code == 401:  # Unauthorized
                    # Handle token expiration and update.
                    token_response = get_token(**kwargs)
                        # Call get_auth_header() function with the "access_token" from the token_response.
                    headers = self.authentication.get_auth_header(access_token=token_response["access_token"])
                    logging.info(
                        {"Message":"Token has been refreshed for paginated album details"}
                        )

                # Convert the response to json using the json() method.
                response_json = response.json()
                album_data.extend(response_json["items"])
            return album_data

        except Exception as err:
            logging.error(
            {
                "Message": f"Error occurred during request {err} for request url {request_url}",
            })
            return []
        
        
        
# main()function

kwargs = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "url": URL_TOKEN,
    }

token = authentication.get_token(**kwargs)

new_releases = endpoints.get_paginated_new_releases(
    base_url=URL_NEW_RELEASES,
    access_token=token["access_token"],
    get_token=authentication.get_token,
    **kwargs,
)


albums_ids = [album["id"] for album in new_releases]


album_items = {}


for album_id in albums_ids:
    
    album_data = endpoints.get_paginated_album_tracks(
        base_url=URL_ALBUM_TRACKS,
        access_token=token["access_token"],
        album_id=album_id,
        get_token=authentication.get_token,
        **kwargs,
    )
    
    album_items[album_id] = album_data
    logging.info({
        "message" : f"Album id {album_id} processed."
    })

sc = SparkContext()
glueContext = GlueContext(sc)
# Spark Session, the entry point to programming Spark with the Dataset and DataFrame API
spark = glueContext.spark_session
# Generate a Spark DataFrame from dict
dest_pd = spark.createDataFrame(album_items)
# Generate a  Glue DynamicFrame from Spark DataFrame
dest_df = DynamicFrame.fromDF(dest_pd, glueContext, "dest_df")
# Instanciate a Glue Job with the Glue context
job = Job(glueContext)
# Initialize the Job
job.init(args["JOB_NAME"], args)
connection_options = (
    {
            "path": f"s3://{args['s3_bucket']}/{args['target_path']}/",
    }
)

# Write the previous Glue DynamicFrame to a parquet file in a given S3 path
datasink = glueContext.write_dynamic_frame.from_options(
    frame=dest_df,
    connection_type="s3",
    format="json",
    connection_options=connection_options,
    transformation_ctx="datasink",
)

# with open("./dags/transform.json", "w") as file:
#             file.write(json.dumps(album_items))