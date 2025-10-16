from sqlalchemy import text
from .schemas import SchemaLoader
from connectors.SQL.PostgreSQL.psql_connector import PostgresConnector
from connectors.SQL.PostgreSQL.psql_config import PostgresConfig

fk_db_cred = {
        "DB_HOST": "localhost",
        "DB_NAME": "fakestream",
        "DB_USER": "testadmin",
        "DB_PASSWORD": "password",
        "PORT": 55432,
        "SSL_MODE": "disable"
    }

print(fk_db_cred['DB_NAME'])
config = PostgresConfig(
    host=fk_db_cred["DB_HOST"], 
    database=fk_db_cred['DB_NAME'], 
    username=fk_db_cred["DB_USER"],
    password=fk_db_cred["DB_PASSWORD"],
    port=fk_db_cred['PORT'],
    ssl_mode=fk_db_cred["SSL_MODE"]
)

connector = PostgresConnector(config=config)
schema_loader = SchemaLoader(engine=connector.engine)
schemas = schema_loader.ddl_statements

print("✅ Loaded schema definitions:")
for table_name in schemas.keys():
    print("-", table_name)

# drop and build the tables using ddl from schemas

for table_name, ddl in schemas.items():
    try:
        print(f"\n⚙️ Rebuilding table: {table_name}")
        # Drop the table if it exists
        try:
            connector.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        except Exception as e:
            print(f"Failed to drop table {table_name}")
        # Recreate it using the provided DDL
        print(ddl)
        connector.execute(ddl)
        print(f"✅ {table_name} recreated successfully.")
    except Exception as e:
        print(f"❌ Failed to rebuild {table_name}: {e}")

connector.close()


