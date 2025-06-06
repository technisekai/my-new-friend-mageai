from itertools import groupby
from operator import itemgetter

def convert_to_scd2(data: list[dict], primary_key_name: str):
    result = []
    for key, group_items in groupby(data, key=itemgetter(primary_key_name)):
        group_list = list(group_items)
        for i, item in enumerate(group_list):
            item['is_active'] = (i == len(group_list) - 1)
            result.append(item)
    return data