"""
File untuk test postgresql connection

Untuk sementara, gw test pake postgresql running on docker and local postgresql dlu
Karena gw lg di linux, gw cm bs test postgresql sm mysql

TO-DO
1. Create postgresql database local and docker
2. Get db credentials and store in a dictionary for now
3. Create a menu that takes user input
4. Create instance of DBConfig and pass user input into the DBConfig instance
5. Check if PostgresConfig works as intended
6. Create instance of connector and pass PostgresConfig instance as a parameter
7. Connect to db from python
8. Check if PostgresConnector works as intended
9. If both things work, try querying from the database
10. Make a script to query all the tables in the database (full load)
11. Then, make a batch load script to query all the new rows

REQUIREMENTS
1. Make PostgreSQL db, fill it with sample data
2. Create fake data sampling script that runs every 1 hour
"""

from .psql_config import PostgresConfig
from .psql_connector import PostgresConnector
from sqlalchemy import text
import sqlparse


def is_select_query(query: str)->bool:
    query = query.strip()
    if not query:
        return False
    
    parsed = sqlparse.parse(query)
    if not parsed:
        return False
    stmt = parsed[0]
    for tkn in stmt.tokens:
        if tkn.ttype in sqlparse.tokens.Whitespace or tkn.ttype in sqlparse.tokens.Comment:
            continue
        return tkn.value.upper() in ('SELECT', 'WITH')
    return False
    
    

if __name__ == '__main__':
    host = input("DATABASE HOST: ")
    name = input("DATABASE NAME: ")
    user = input("DATABASE USER: ")
    pw = input("DATABASE PASSWORD: ")
    port = input("DATABASE PORT: ")
    sslmode = input("SSL mode [disable/require]: ") or "require"

    USER_DB_CRED = {
        "DB_HOST": host,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": pw,
        "PORT": port,
        "SSL_MODE": sslmode
    }
    nw_db_cred = {
        "DB_HOST": "localhost",
        "DB_NAME": "northwind",
        "DB_USER": "testadmin",
        "DB_PASSWORD": "password",
        "PORT": 55433,
    }

    fk_db_cred = {
        "DB_HOST": "localhost",
        "DB_NAME": "fakestream",
        "DB_USER": "testadmin",
        "DB_PASSWORD": "password",
        "PORT": 55432,
    }

    print(USER_DB_CRED['DB_NAME'])
    config = PostgresConfig(
        host=USER_DB_CRED["DB_HOST"], 
        database=USER_DB_CRED['DB_NAME'], 
        username=USER_DB_CRED["DB_USER"],
        password=USER_DB_CRED["DB_PASSWORD"],
        port=USER_DB_CRED['PORT'],
        ssl_mode=USER_DB_CRED["SSL_MODE"]
    )

    cnn_str = config.get_connection_string()
    print(cnn_str)

    connector = PostgresConnector(config=config)
    print("="*30)
    print("Connecting to " + cnn_str)
    print(connector.connect())
    print("="*30)
    print("Connected")
    print(f"Database info\n{connector.get_database_info()}")
    print("="*30)

    print(f"Tables in {config.database}")
    tables = connector.get_table_names()
    for table in tables:
        cols = connector.get_table_columns(table)
        print(f"{table}\n" + "\n".join(f" - {col}" for col in cols))

    print("="*30)
    print("TESTING CONNECTIVITY")
    user_query = input("SQL Query: ")
    
    if is_select_query(user_query):
        try:
            print(connector.fetch_one(user_query))  # pass string directly
        except Exception as e:
            print(f"Query failed: {e}")
    print("="*30)
