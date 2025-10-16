from airflow.sdk import dag, task
import pendulum
import pandas as pd
import logging
from datetime import timedelta, datetime
import sys
PROJECT_ROOT = "/home/jonut/Projects/ultimate-data-engineering-project"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from OLTP_simulator.generator.data.schemas import SchemaLoader
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_config import PostgresConfig
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_connector import PostgresConnector

@dag(
    dag_id="batch_ingestion_dag",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 10, 16),
    catchup=False,
    tags=['batch_processing', 'ingestion'],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
    }
)
def batch_ingestion_pipeline():
    """
    This dag will ingest newly made data based on timestamp based on updated_at column in our database
    - customers, accounts, transactions all have updated_at column
    - we fetch rows where updated_at is above the previous batch time, ie an hour before
    - this way we get new fresh hourly data
    - we update the new timestamp to be this hour at the end
    """
    @task
    def connect_to_db():
        """Establish database connection and ensure schema exists"""
        DB_CONFIG = {
            "host": "localhost",
            "database": "fakestream",
            "username": "testadmin",
            "password": "password",
            "port": 55432,
            "ssl_mode": "disable"
        }
        
        try:
            connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
            connector.connect()
            
            # Ensure schema exists
            SchemaLoader(connector.engine)
            logging.info(f"✅ Database connection established")
            return DB_CONFIG
            
        except Exception as e:
            raise
    
    @task
    def fetch_new_rows(DB_CONFIG: dict, latest_ts: datetime):
        """
        Fetches new rows from updated_at column
        """
        try:
            connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
            print(connector.engine.dialect.name)   # postgresql
            print(connector.engine.dialect.driver) # psycopg2
            # get all tables in 
            db_tables_info = {}
            db_tables = connector.get_table_names()
            
            print(db_tables)
            if not db_tables:
                logging.warning("No tables found in the database.")
                return
            
            for tbl in db_tables:
                
                query = f"""
                    SELECT *
                    FROM {tbl}
                    WHERE updated_at > %(latest_ts)s
                """

                # fetch data
                
                new_data = pd.read_sql(query, con=connector.engine, params={
                    "latest_ts": latest_ts
                })
                print(new_data.head(1))
                if new_data.empty:
                    logging.warning(f"No new rows for {tbl}")
                    continue
                else:
                    # get info
                    tbl_len = len(new_data)
                    db_tables_info[tbl] = tbl_len
                
                # load new data to s3
                try:
                    connector.load(df=new_data, table=tbl)
                    print("Successfully loaded new data to S3")
                    
                except Exception as e:
                    print(f"❌ Failed loading to S3: {e}")
                    raise
            latest_ts = datetime.now()
            return {"latest_ts": latest_ts, "db_info": db_tables_info}
        except Exception as e:
            logging.error(f"❌ Connection failed: {e}")
            raise
    
    @task
    def generate_hourly_report(result):
        """Generate summary report of hourly new data loading to S3"""
        print(result)
        latest_ts = result["latest_ts"]
        db_info = result["db_info"]
        
        print("\n" + "="*60)
        print("📊 HOURLY BATCH INGESTION REPORT")
        print("="*60)
        print(f"Last ingested at: {latest_ts}")
        
        for k,v in db_info.items():
            print(f"Table: {k} | Rows ingested: {v}")
        print("="*60)
        
    last_ts = datetime.now() - timedelta(hours=1)
    db_conn = connect_to_db()
    result = fetch_new_rows(db_conn, last_ts)
    report = generate_hourly_report(result)
    db_conn >> result >> report
    
dag_instance=batch_ingestion_pipeline()