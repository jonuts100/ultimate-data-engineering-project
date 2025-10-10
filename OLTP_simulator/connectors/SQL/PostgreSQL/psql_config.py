from pydantic import  Field
from typing import Optional, Dict, Any
from ..database_config import DatabaseConfig

class PostgresConfig(DatabaseConfig):
    '''
    PostgreSQL DB Config
    '''
    
    ssl_mode: Optional[str] = Field(None, description="SSL mode: disable, allow, prefer, require, verify-ca, verify-full")
    db_schema: Optional[str] = Field("public", description="Default schema")
    application_name: Optional[str] = Field("python_app", description="Application name for logging")
    
    def get_default_port(self) -> int:
        return 5432

    def get_connection_string(self):
        '''
        Genereate PostgreSQL connection string
        '''
        
        base = f"postgresql+psycopg2://{self.username}:{self._encode_password()}@{self.host}:{self.get_port()}/{self.database}"
        params = []
        if self.connect_timeout:
            params.append(f"connect_timeout={self.connect_timeout}")
        if self.ssl_mode:
            params.append(f"sslmode={self.ssl_mode}")
        if self.application_name:
            params.append(f"application_name={self.application_name}")
        
        
        return f"{base}?{('&'.join(params))}" if params else base
    
    def get_engine_kwargs(self) -> Dict[str, Any]:
        """PostgreSQL engine configuration."""
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_pre_ping": self.pool_pre_ping,
            "connect_args": {
                "options": f"-c search_path={self.db_schema}" if self.db_schema else {}
            }
        }
        