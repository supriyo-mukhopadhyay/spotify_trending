# from typing import Callable

# import requests
# from authentication import get_auth_header, get_token
# from endpoints import get_paginated_album_tracks, get_paginated_new_releases


# import os

# from dotenv import load_dotenv
# load_dotenv()

# CLIENT_ID = os.getenv("CLIENT_ID")
# CLIENT_SECRET = os.getenv("CLIENT_SECRET")


# URL_TOKEN = "https://accounts.spotify.com/api/token"
# URL_NEW_RELEASES = "https://api.spotify.com/v1/browse/new-releases"
# URL_ALBUM_TRACKS = "https://api.spotify.com/v1/albums"


# kwargs = {
#         "client_id": CLIENT_ID,
#         "client_secret": CLIENT_SECRET,
#         "url": URL_TOKEN,
#     }

# token = get_token(**kwargs)

# new_releases = get_paginated_new_releases(
#     base_url=URL_NEW_RELEASES,
#     access_token=token["access_token"],
#     get_token=get_token,
#     **kwargs,
# )

# # print(new_releases)

# albums_ids = [album["id"] for album in new_releases]
# # print(albums_ids)

# album_items = {}

# for album_id in albums_ids:
#         album_data = get_paginated_album_tracks(
#             base_url=URL_ALBUM_TRACKS,
#             access_token=token["access_token"],
#             album_id=album_id,
#             get_token=get_token,
#         )


import pandas as pd
import json
import csv
with open('myfile.json') as jf:
    d = json.load(jf)

Ed = d['emp_details']

df = open('data_file.csv', 'w')

cw = csv.writer(df)

c = 0

for emp in Ed:
    if c == 0:

        # Writing headers of CSV file
        h = emp.keys()
        cw.writerow(h)
        c += 1

    # Writing data of CSV file
    cw.writerow(emp.values())

df.close()