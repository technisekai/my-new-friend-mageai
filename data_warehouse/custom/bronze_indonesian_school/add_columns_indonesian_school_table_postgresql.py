import json
from mage_ai.data_preparation.shared.secrets import get_secret_value
from data_warehouse.cores.postgresql import postgresql_connection, add_columns_postgresql

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def add_columns_indonesian_school_table_postgresql(values, **kwargs):
    """
    Checking row by row and add columns if not exists in table schema.
    Params:
        values: The output from the upstream parent block. format [{}, {}, ..., {}]
        args: The output from any additional upstream blocks (if applicable)
    Returns:
        None. alter table add columns if not exists
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
    # Schema checking row by row. if key not exists in table schema then alter table
    for value in values:
        add_columns_postgresql(
            source_data=value,
            destination_schema_name=kwargs['bronze_indonesian_school_config'].get("destination_schema_name"),
            destination_table_name=kwargs['bronze_indonesian_school_config'].get("destination_table_name"),
            destination_connect=dwh_connection
        )
    # Close connection
    dwh_connection.close()

    return values
