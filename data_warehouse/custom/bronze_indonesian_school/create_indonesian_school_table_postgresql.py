import json
from mage_ai.data_preparation.shared.secrets import get_secret_value
from data_warehouse.cores.postgresql import postgresql_connection, create_table_postgresql

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def create_indonesian_school_table_postgresql(values, **kwargs):
    """
    Create table for the data if not exists.
    Params:
        values: The output from the upstream parent block. format [{}, {}, ..., {}]
        args: The output from any additional upstream blocks (if applicable)
    Returns:
        None. created table if not exists
    """
    # Create connection to database
    data_key_name = kwargs['bronze_indonesian_school_config'].get("data_key_name")
    db_credential = json.loads(get_secret_value('database_connections')) \
        .get("postgresql_data_warehouse_public")
    dwh_connection = postgresql_connection(
        host=db_credential.get("host"), 
        port=db_credential.get("port"), 
        user=db_credential.get("user"), 
        password=db_credential.get("password"), 
        database=db_credential.get("database")
    )
    # Create table if not exists
    create_table_postgresql(
        source_data=values[-1],
        destination_schema_name=kwargs['bronze_indonesian_school_config'].get("destination_schema_name"),
        destination_table_name=kwargs['bronze_indonesian_school_config'].get("destination_table_name"),
        destination_connect=dwh_connection,
        if_exists='replace'
    )
    # Close connection
    dwh_connection.close()

    return values

