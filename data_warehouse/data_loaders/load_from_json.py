import os
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_from_url(**kwargs):
    print(json.loads(get_secret_value('database_connections'))["widi"])