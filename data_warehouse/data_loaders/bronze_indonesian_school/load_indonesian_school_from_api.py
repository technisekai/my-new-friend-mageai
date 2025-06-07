import os
import requests
from data_warehouse.cores.api import get_from_url

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_indonesian_school_from_url(**kwargs):
    """
    Get data from indonesian school api 
    Params:
        **kwargs: The output from any additional upstream blocks (if applicable)
    Returns:
        dictionary with format {
            'key': '...',
            ...
            'data': [{}, {}]
        }
    """
    # Get data from api
    url = kwargs['bronze_indonesian_school_config'].get("api_url")
    return get_from_url(url)

@test
def test_data_type(result):
    assert 'dict' == type(result).__name__