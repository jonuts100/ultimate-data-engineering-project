from sqlalchemy import create_engine, text, Engine
from sqlalchemy.engine import CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field, field_validator, SecretStr, ConfigDict
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
from database_config import DatabaseConfig
from urllib.parse import quote_plus
import logging
from contextlib import contextmanager

class DatabaseConnector(ABC):
    """
    Abstract base class for database connectors.
    
    Provides common database operations while allowing
    database-specific implementations through inheritance.
    """
    
    def __init__(self, db_config: DatabaseConfig):
        """
        Initialize connector with configuration.
        
        Args:
            db_config: Database configuration object (PostgresConfig, MySQLConfig, etc.)
        """
        self.db_config = db_config
        self.engine: Optional[Engine] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
    # === Connection Management
    @abstractmethod
    def connect(self) -> Engine:
        """
        Establish database connection
        Must be established by subclasses to implement database-specific connection logic
        
        Returns:
            SQLAlchemy Engine object
            
        Raises:
            SQLAlchemyError: If connection fails
        """
        pass
    
    def is_connected(self) -> bool:
        """Checking if database connection established """
        return self.engine is not None
        
        
    def test_connection(self) -> bool:
        """
        Testing connection to Database
        
        Returns:
            True if connection works, False otherwise
        """  
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
        
    @contextmanager
    def get_connection(self):
        """
        Context manager for safe connection handling
        
        Automatically handles commit/rollback and connection cleanup.
        
        Usage:
            with connector.get_connection() as conn:
                result = conn.execute(text("SELECT * FROM users"))
        
        Yields:
            SQLAlchemy Connection object
        """
        if not self.is_connected():
            self.connect()
            
        conn = self.engine.connect()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Transaction failed and rolled back: {e}")
            raise
        finally:
            conn.close()
        
        
    def close(self):
        """
        Close and cleanup the current established database connection 
        """
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.logger.info("Database connection closed")
            
    # === Query execution
    
    def fetch(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]: 
        """
        Executes user-given SELECT query on the database
        
        Args:
            query: SQL SELECT query string (use :param_name for parameters)
            params: Optional dictionary of parameters for safe parameterized queries
            
        Returns:
            List of row dictionaries
            
        Example:
            results = connector.fetch(
                "SELECT * FROM users WHERE age > :min_age",
                {"min_age": 18}
            )
            # Returns: [{"id": 1, "name": "Alice", "age": 25}, ...]
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]
            
        except SQLAlchemyError as e:
            self.logger.error(f"Fetch query failed: {e}")
            raise
        
    def fetch_one(self, query: str, params: Optional[Dict[str, Any]] = None) ->Dict[str, Any]: 
        """
        Fetch function but returns just the first item
        """
        res = self.fetch(query, params)
        return res[0] if res else None
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> CursorResult:
        """
        Executes INSERT, UPDATE, DELETE queries 
        
        Args:
            query: SQL query string (use :param_name for parameters)
            params: Optional dictionary of parameters for safe parameterized queries
            
        Returns:
            CursorResult object with rowcount and other metadata
            
        Example:
            result = connector.execute(
                "UPDATE users SET status = :status WHERE id = :user_id",
                {"status": "active", "user_id": 123}
            )
            print(f"Updated {result.rowcount} rows")
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                return result
        except SQLAlchemyError as e:
            self.logger.error(f"Execute query failed: {e}")
            raise
        
    def execute_many(
        self, 
        query: str, 
        params_list: List[Dict[str, Any]]
    ) -> int:
        """
        Execute same query with multiple parameter sets (bulk operation).
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            
        Returns:
            Total number of affected rows
            
        Example:
            connector.execute_many(
                "INSERT INTO users (name, age) VALUES (:name, :age)",
                [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30},
                ]
            )
        """
        total_rows = 0
        try:
            with self.get_connection() as conn:
                for params in params_list:
                    result = conn.execute(text(query), params)
                    total_rows += result.rowcount
            return total_rows
        except SQLAlchemyError as e:
            self.logger.error(f"Execute many failed: {e}")
            raise
    
    
    # ==================== DATABASE INTROSPECTION ====================
    
    @abstractmethod
    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        """
        Get list of tables in the database.
        
        Must be implemented by subclasses as syntax varies by database.
        
        Args:
            schema: Optional schema name (default behavior varies by database)
            
        Returns:
            List of table names
        """
        pass
    
    @abstractmethod
    def get_table_columns(
        self, 
        table_name: str, 
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get column information for a specific table.
        
        Must be implemented by subclasses as syntax varies by database.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            List of dictionaries with column info (name, type, nullable, etc.)
        """
        pass
    
    @abstractmethod
    def get_database_version(self) -> str:
        """
        Get database server version string.
        
        Must be implemented by subclasses as each database has different syntax.
        
        Returns:
            Version string
        """
        pass
    
    def get_database_info(self, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive database information.
        
        Returns:
            Dictionary with database metadata (version, tables, etc.)
        """
        try:
            return {
                "version": self.get_database_version(),
                "host": self.db_config.host,
                "port": self.db_config.get_port(),
                "database": self.db_config.database,
                "tables": self.get_table_names(schema),
                "connection_status": "connected" if self.is_connected() else "disconnected"
            }
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {"error": str(e)}
    
    # ==================== CONTEXT MANAGER SUPPORT ====================
    
    def __enter__(self):
        """Support 'with' statement for automatic connection management."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on exit from 'with' block."""
        self.close()
    
    def __repr__(self) -> str:
        """String representation hiding sensitive info."""
        status = "connected" if self.is_connected() else "disconnected"
        return (
            f"{self.__class__.__name__}("
            f"host='{self.db_config.host}', "
            f"database='{self.db_config.database}', "
            f"status='{status}')"
        )
    
    def __del__(self):
        """Cleanup on object deletion."""
        self.close()
