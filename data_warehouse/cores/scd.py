from itertools import groupby
from operator import itemgetter

def convert_to_scd2(data: list[dict], primary_key_name: str) -> list[dict]:
    """
    Convert data to SCD type 2 with add is_active column
    params:
        - data: data will be injected
        - primary_key_name: unique column name in data source
    Returns:
        list dictionary with is_active column. [{..., 'is_active': Bool, ...}]
    """
    result = []
    for key, group_items in groupby(data, key=itemgetter(primary_key_name)):
        group_list = list(group_items)
        for i, item in enumerate(group_list):
            item['is_active'] = (i == len(group_list) - 1)
            result.append(item)
    print(f"INF successfully generated scd type 2")
    return data