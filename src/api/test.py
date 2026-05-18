from typing import Callable

import requests
from authentication import get_auth_header, get_token
from endpoints import get_paginated_album_tracks, get_paginated_new_releases


import os

from dotenv import load_dotenv
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


URL_TOKEN = "https://accounts.spotify.com/api/token"
URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"
URL_ALBUM_TRACKS = "https://api.spotify.com/v1/albums"


kwargs = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "url": URL_TOKEN,
    }

token = get_token(**kwargs)

new_releases = get_paginated_new_releases(
    base_url=URL_NEW_RELEASES,
    access_token=token["access_token"],
    get_token=get_token,
    **kwargs,
)

# print(new_releases)

albums_ids = [album["id"] for album in new_releases]
# print(albums_ids)

album_items = {}

for album_id in albums_ids:
        album_data = get_paginated_album_tracks(
            base_url=URL_ALBUM_TRACKS,
            access_token=token["access_token"],
            album_id=album_id,
            get_token=get_token,
            **kwargs,
        )