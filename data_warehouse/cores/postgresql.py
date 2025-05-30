import psycopg2
from typing import Literal
from psycopg2.extras import execute_values, Json

def postgresql_connection(host: str, port: int, user: str, password: str, database: str):
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return connection

def get_from_postgresql(destination_connect: postgresql_connection, query: str):
    destination_cursor = destination_connect.cursor()
    destination_cursor.execute(query)
    columns = [desc[0] for desc in destination_cursor.description]
    values = destination_cursor.fetchall()
    result = [dict(zip(columns, x)) for x in values]
    return result

def insert_into_postgres(
        destination_connect: postgresql_connection, 
        destination_schema_name: str, 
        destination_table_name: str, 
        data: list[dict], 
        chunksize: int = 1000,
        mode: Literal['batch', 'single'] = 'batch'
):
    destination_cursor = destination_connect.cursor()
    if mode == 'batch':
        for idx in range(0, len(data), chunksize):
            columns = data[idx].keys()
            values = [[Json(row.get(col, None)) if isinstance(row.get(col, None), (dict, list)) \
                    else  row.get(col, None) \
                        for col in columns] for row in data[idx:idx+chunksize]]
            query_insert_data = f"""
            insert into {destination_schema_name}.{destination_table_name} ({', '.join(columns)}) values %s
            """
            execute_values(destination_cursor, query_insert_data, values)
            destination_connect.commit()
    else:
        for value in data:
            columns = value.keys()
            values = [
                Json(v) if isinstance(v, (dict, list)) else v
                for v in value.values()
            ]
            query_insert_data = f"""
            insert into {destination_schema_name}.{destination_table_name} ({', '.join(columns)}) values ({', '.join(['%s'] * len(columns))})
            """
            destination_cursor.execute(query_insert_data, values)
            destination_connect.commit()
    destination_cursor.close()

def convert_to_postgresql_schema(source_schema: dict):
    """
    Convert json key-value to postgres schema
    Params:
        - source_schema: source data {'key': 'value'}
    Returns:
        - dict of postgres schema {'column': 'data type'}
    """
    postgresql_schema = {}
    for key in source_schema.keys():
        json_type_name = type(source_schema[key]).__name__
        if json_type_name == 'str':
            postgresql_schema[key] = 'varchar'
        elif json_type_name in ['int', 'float']:
            postgresql_schema[key] = 'numeric'
        elif json_type_name in ['list', 'dict']:
            postgresql_schema[key] = 'jsonb'
        elif json_type_name == 'bool':
            postgresql_schema[key] = 'boolean'
        else:
            raise Exception(f'Err {json_type_name} data type not defined!')
    
    return postgresql_schema

def create_table_postgresql(
        source_data: dict, 
        destination_schema_name: str, 
        destination_table_name: str, 
        destination_connect: postgresql_connection
):
    postgresql_schema = convert_to_postgresql_schema(source_data)
    query_create_table = f"""
    create table if not exists {destination_schema_name}.{destination_table_name} (
        {', '.join([' '.join(x) for x in zip(postgresql_schema.keys(), postgresql_schema.values())])}
    )
    """
    print(f"INF execute the query below {query_create_table}")
    destination_cursor = destination_connect.cursor()
    destination_cursor.execute(query_create_table)
    destination_connect.commit()
    destination_cursor.close()

def add_columns_postgresql(
        source_data: dict, 
        destination_table_name: str, 
        destination_schema_name: str, 
        destination_connect: postgresql_connection
):
    # Get fields not exists in table
    query_filter_schema = f"""
    select 
        column_name 
    from information_schema."columns" c 
    where 1=1
        and table_schema = '{destination_schema_name}'
        and table_name = '{destination_table_name}'
    """
    print(f"INF execute the query below {query_filter_schema}")
    existing_schema = get_from_postgresql(destination_connect, query_filter_schema)
    source_data_not_exists = {k: v for k, v in source_data.items() if k not in [x["column_name"] for x in existing_schema]}
    print(source_data_not_exists)
    postgresql_schema = convert_to_postgresql_schema(source_data_not_exists)
    print(postgresql_schema)
    destination_cursor = destination_connect.cursor()
    for column_name, column_type in zip(postgresql_schema.keys(), postgresql_schema.values()):
        query_alter_table = f"""
        alter table {destination_schema_name}.{destination_table_name} add column {column_name} {column_type};
        """
        print(f"INF execute the query below {query_alter_table}")
        destination_cursor.execute(query_alter_table)
        destination_connect.commit()
    destination_cursor.close()