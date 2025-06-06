import os
import requests
from data_warehouse.cores.api import get_from_url

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_book_science_from_url(**kwargs):
    url = kwargs['books_bronze_config'].get("api_url")
    return get_from_url(url)

@test
def test_data_type(result):
    assert 'dict' == type(result).__name__