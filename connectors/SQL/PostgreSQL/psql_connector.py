from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List, Dict, Any
from psql_config import PostgresConfig
from connector_config import DatabaseConnector

class PostgresConnector(DatabaseConnector):
    """PostgreSQL-specific connector implementation."""
    
    def __init__(self, config: PostgresConfig):
        super().__init__(config)
        self.db_config: PostgresConfig = config  # Type hint for IDE autocomplete
    
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
