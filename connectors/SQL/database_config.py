from pydantic import BaseModel, Field, field_validator, SecretStr, ConfigDict
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from urllib.parse import quote_plus

class DatabaseConfig(ABC, BaseModel):
    '''
    Base configuration for db connection
    '''
    model_config = ConfigDict(
        str_strip_whitespace=True, # auto strip white spaces
        validate_assignment=True, # add validation checker
        extra="forbid" # prevent accidental typos in config
    )
    
    host: str = Field(..., description="DB Host identifier", min_length=1)
    database: str = Field(..., description='DB name', min_length=1)
    username: str = Field(..., min_length=1)
    password: str = SecretStr(Field(..., description="DB Password"))
    #server: str = Field(..., description="DB Server")
    port: Optional[int]= Field(None, description="DB Default Port", gt=0, le=65535)
    #conn_str: str = Field(..., description="DB Connection String")
    
    # connection settings
    connect_timeout: Optional[int] = Field(30, description="Connection timeout in seconds")
    pool_size: Optional[int] = Field(5, description="Connection pool size")
    max_overflow: Optional[int] = Field(10, description="Max overflow connections")
    pool_pre_ping: bool = Field(True, description="Enable connection health checks")
    
    @field_validator("host", "username", "database")
    @classmethod
    def is_not_empty(cls, v: str) -> str:
        '''
        String inputs is not empty check
        '''
        if not v.strip() or not v:
            raise ValueError("Field cannot be empty or whitespace only")
        return v.strip()
    
    # abstract classes -> blueprint to be implemented by instance classes later
    @abstractmethod
    def get_default_port(self) -> int:
        """Return the default port for this database type."""
        pass
    
    @abstractmethod
    def get_connection_string(self) -> str:
        """Generate database connection string."""
        pass
    
    @abstractmethod
    def get_engine_kwargs(self) -> Dict[str, Any]:
        """Return SQLAlchemy engine configuration."""
        pass
    
    def get_port(self) -> int:
        """Get port, using default if not specified."""
        return self.port if self.port is not None else self.get_default_port()
    
    def _encode_password(self) -> str:
        """URL-encode password for safe inclusion in connection string."""
        return quote_plus(self.password.get_secret_value()) # pake get secret values karena dihide pake secretstr
    
    def __repr__(self) -> str:
        """Override repr to hide password."""
        return (
            f"{self.__class__.__name__}("
            f"host='{self.host}', "
            f"port={self.get_port()}, "
            f"database='{self.database}', "
            f"username='{self.username}')"
        )
