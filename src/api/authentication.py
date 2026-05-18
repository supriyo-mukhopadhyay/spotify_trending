import json
from typing import Any, Dict
import requests
import time
from dotenv import load_dotenv
import os

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
URL_TOKEN = "https://accounts.spotify.com/api/token"
URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"
URL_ALBUM_TRACKS = "https://api.spotify.com/v1/albums"

def get_token(client_id: str, client_secret: str, url: str) -> Dict[Any, Any]:
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
        print(f"Error: {err}")
        return {}
    

def get_auth_header(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}

kwargs = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "url": URL_TOKEN,
    }

token = get_token(**kwargs)
# Define the Spotify API endpoint
endpoint = 'https://api.spotify.com/v1/browse/new-releases'

print(token["access_token"])

headers = get_auth_header(access_token=token["access_token"])

# Define the number of requests to make
num_requests = 200

# Define the interval between requests (in seconds)
request_interval = 0.2  # Adjust as needed based on the API rate limit

# Store the timestamps of successful requests
success_timestamps = []

# Make repeated requests to the endpoint
for i in range(num_requests):
    # Make the request
    response = requests.get(url=endpoint, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        success_timestamps.append(time.time())
    else:        
        print(f'Request {i+1}: Failed with code {response.status_code}')
    
    # Wait for the specified interval before making the next request
    time.sleep(request_interval)

# Calculate the time between successful requests
if len(success_timestamps) > 1:
    time_gaps = [success_timestamps[i] - success_timestamps[i-1] for i in range(1, len(success_timestamps))]
    print(f'Average time between successful requests: {sum(time_gaps) / len(time_gaps):.2f} seconds')
else:
    print('At least two successful requests are needed to calculate the time between requests.')