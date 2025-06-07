import json
from data_warehouse.cores.scd import convert_to_scd2

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def convert_indonesian_school_to_scd2(results, **kwargs):
    """
    Convert data to scd type 2.
    Params:
        results: The output from the upstream parent block. format [{}, {}, ..., {}]
        args: The output from any additional upstream blocks (if applicable)
    Returns:
        dictionary with format [{..., 'is_active': bool, ...}]
    """
    # Add is_active key to convert data into scd type 2
    data_key_name = kwargs['bronze_indonesian_school_config'].get("data_key_name")
    return convert_to_scd2(
        data=results[data_key_name], 
        primary_key_name=kwargs['bronze_indonesian_school_config'].get("source_unique_key")
    )

