from typing import Callable

import requests
from authentication import get_auth_header

def get_paginated_new_releases(
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
    headers = get_auth_header(access_token=access_token)
    request_url = base_url
    new_releases_data = []

    try:
        while request_url:
            print(f"Requesting to: {request_url}")
            response = requests.get(url=request_url, headers=headers)

            if response.status_code == 401:
                token_response = get_token(**kwargs)
                headers = get_auth_header(access_token=token_response["access_token"])
            # print(response)
            
            response_json = response.json()
            new_releases_data.extend(response_json["albums"]["items"])
            request_url = response_json["albums"]["next"]
        return new_releases_data

    except Exception as err:
        print(f"Error occurred during request: {err}")
        return []
    