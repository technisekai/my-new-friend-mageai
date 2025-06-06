import json
from mage_ai.data_preparation.shared.secrets import get_secret_value
from data_warehouse.cores.postgresql import postgresql_connection, create_table_postgresql, add_columns_postgresql

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def prepare_table_postgresql(results, **kwargs):
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
    create_table_postgresql(
        source_data=results["works"][-1],
        destination_schema_name="bronze",
        destination_table_name="api_book_science",
        destination_connect=dwh_connection
    )
    dwh_connection.close()

    return results["works"]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert type(output).__name__ is 'list', 'The output is undefined'
