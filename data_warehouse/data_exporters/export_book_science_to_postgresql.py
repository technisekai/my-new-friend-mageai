from data_warehouse.cores.postgresql import postgresql_connection, insert_into_postgres
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgresql(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    # Specify your custom logic here
    db_credential = json.loads(get_secret_value('database_connections')) \
        .get("postgresql_data_warehouse_public")
    dwh_connection = postgresql_connection(
        host=db_credential.get("host"), 
        port=db_credential.get("port"), 
        user=db_credential.get("user"), 
        password=db_credential.get("password"), 
        database=db_credential.get("database")
    )
    insert_into_postgres(
        destination_connect=dwh_connection, 
        destination_schema_name="bronze",
        destination_table_name="api_book_science",
        data=data, 
    )


