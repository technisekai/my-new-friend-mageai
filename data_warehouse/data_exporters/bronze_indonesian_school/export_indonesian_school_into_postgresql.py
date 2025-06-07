from data_warehouse.cores.postgresql import postgresql_connection, insert_into_postgres
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_indonesian_school_to_postgresql(data, *args, **kwargs):
    """
    Exports data to some source.
    Params:
        data: The output from the upstream parent block. format [{}, {}, ..., {}]
        args: The output from any additional upstream blocks (if applicable)
    Returns:
        None. data inserted to database
    """
    # Create connection to database
    db_credential = json.loads(get_secret_value('database_connections')) \
        .get("postgresql_data_warehouse_public")
    dwh_connection = postgresql_connection(
        host=db_credential.get("host"), 
        port=db_credential.get("port"), 
        user=db_credential.get("user"), 
        password=db_credential.get("password"), 
        database=db_credential.get("database")
    )
    # Insert data to database
    insert_into_postgres(
        destination_connect=dwh_connection, 
        destination_schema_name=kwargs['bronze_indonesian_school_config'].get("destination_schema_name"),
        destination_table_name=kwargs['bronze_indonesian_school_config'].get("destination_table_name"),
        unique_key_name=kwargs['bronze_indonesian_school_config'].get("source_unique_key"),
        data=data,
        mode='single'
    )
    # Close connection
    dwh_connection.close()


