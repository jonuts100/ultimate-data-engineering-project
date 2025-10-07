from sqlalchemy import create_engine
import psycopg2

class DatabaseLoader:
    def __init__(self, platform="postgresql", user="postgres", password="Hoki100100", host="localhost", port=None, database="finance_db"):
        self.conn_str = self.get_connection_string(platform, user, password, host, port, database)
        self.engine = create_engine(self.conn_str)

    def get_connection_string(self, platform: str, user: str, password: str, host: str, port: int | None, database: str) -> str:
        """Return a full SQLAlchemy database connection string based on the platform."""
        platform = platform.lower()
        
        if platform == "postgresql":
            port = port or 5432
            conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        
        elif platform == "mysql":
            port = port or 3306
            conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        
        elif platform == "mssql":
            port = port or 1433
            # Note: requires `pyodbc` and the proper driver installed
            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{host},{port}/{database}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
        
        else:
            raise ValueError(f"Unsupported platform: {platform}")

        return conn_str
