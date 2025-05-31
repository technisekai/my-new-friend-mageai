import requests
import json

def get_from_url(url: str, headers: dict=None, data: dict=None) -> dict:
    """
    Get data from api
    params:
        url: endpoint 
        headers: headers request
    returns:
        json response in dictionary
    """
    if headers:
        response = requests.get(url, headers=headers)
    else:
        response = requests.get(url)

    return json.loads(response.json())