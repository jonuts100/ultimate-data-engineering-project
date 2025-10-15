import pandas as pd
import io
import json
import boto3
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List, Dict, Any
from .psql_config import PostgresConfig
from ..connector_config import DatabaseConnector

class PostgresConnector(DatabaseConnector):
    """PostgreSQL-specific connector implementation."""
    
    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.db_config: PostgresConfig = config  # Type hint for IDE autocomplete
        self.connect()
    def connect(self) -> Engine:
        """Establish PostgreSQL connection."""
        try:
            self.engine = create_engine(
                self.db_config.get_connection_string(),
                **self.db_config.get_engine_kwargs()
            )
            self.logger.info(
                f"Connected to PostgreSQL: {self.db_config.database} "
                f"at {self.db_config.host}:{self.db_config.get_port()}"
            )
            return self.engine
        except SQLAlchemyError as e:
            self.logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        """
        Get PostgreSQL table names from specified schema.
        
        Args:
            schema: Schema name (defaults to 'public')
        """
        schema = schema or "public"
        query = """
            SELECT tablename 
            FROM pg_catalog.pg_tables 
            WHERE schemaname = :schema
            ORDER BY tablename
        """
        results = self.fetch(query, {"schema": schema})
        return [row["tablename"] for row in results]
    
    def get_table_columns(
        self, 
        table_name: str, 
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get PostgreSQL column information for a table.
        
        Returns column name, data type, nullable status, and default value.
        """
        schema = schema or "public"
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table_name
            ORDER BY ordinal_position
        """
        return self.fetch(query, {"schema": schema, "table_name": table_name})
    
    def get_database_version(self) -> str:
        """Get PostgreSQL server version."""
        result = self.fetch_one("SELECT version()")
        return result["version"] if result else "Unknown"
    
    # PostgreSQL-specific methods
    # VACUUM removes old, unused data (kind of like taking out the trash).
    # ANALYZE updates Postgres’s knowledge about the data (helps speed up queries).
    # If you don’t pass a table name, it cleans the whole database.
    # def vacuum_analyze(self, table_name: Optional[str] = None):
    #     """
    #     Run VACUUM ANALYZE for maintenance (PostgreSQL-specific).
        
    #     Args:
    #         table_name: Optional table name. If None, analyzes entire database.
    #     """
    #     query = f"VACUUM ANALYZE {table_name}" if table_name else "VACUUM ANALYZE"
    #     with self.get_connection() as conn:
    #         # VACUUM cannot run inside a transaction
    #         conn.execute(text("COMMIT"))
    #         conn.execute(text(query))
    #     self.logger.info(f"VACUUM ANALYZE completed for {table_name or 'entire database'}")
    
    def get_table_size(self, table_name: str, schema: str = "public") -> Dict[str, Any]:
        """Get size information for a PostgreSQL table."""
        query = """
            SELECT 
                pg_size_pretty(pg_total_relation_size(:full_table)) as total_size,
                pg_size_pretty(pg_relation_size(:full_table)) as table_size,
                pg_size_pretty(pg_total_relation_size(:full_table) - pg_relation_size(:full_table)) as indexes_size
        """
        full_table = f"{schema}.{table_name}"
        return self.fetch_one(query, {"full_table": full_table})
    
    def extract(self):
        # get all tables in the database
        db_name = self.db_config.database
        db_tables = self.get_table_names()
        # get all rows/data from all tables
        table_datas = {}
        
        os.makedirs(f"files/{db_name}", exist_ok=True)

        
        for table in db_tables:
            # load it as a pd DataFrame
            query = f"SELECT * from {table}"
            df = pd.read_sql(query, con=self.engine)
            table_datas[table] = df
            
            print(table + ":")
            print(df)
            print("="*50)
            
            self.logger.info(f"Saved {table}")
            self.load(df, table)

    def load(self, df: pd.DataFrame, table):
        try:
            # get s3 config
            content = open("connectors/env/config.json")
            config = json.load(content)
            access_key = config["access_key"]
            sac_key = config["secret_access_key"]
            # mine is hidden in a file
            # save to s3
            upload_file_bucket = 'jojo-connector-bucket'
            upload_file_key = 'test/bronze/' + str(table) + f"/{str(table)}"
            filepath =  upload_file_key + ".csv"
            rows_imported = 0
            print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {table}')
            s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=sac_key, region_name="ap-southeast-2")
            with io.StringIO() as buffer:
                df.to_csv(buffer, index=False)
                response = s3_client.put_object(
                    Bucket=upload_file_bucket, Key=filepath, Body=buffer.getvalue()
                )
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                else:
                    print(f"Unsuccessful S3 put_object response. Status - {status}")
                rows_imported += len(df)
                print("Data imported successful")
        except Exception as e:
            print("Data load error: " + str(e))
