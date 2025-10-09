import airflow
from airflow.sdk import dag, task
import pendulum
from faker import Faker
import random
from ..schemas import SchemaLoader
from ....connectors.SQL.PostgreSQL.psql_config import PostgresConfig
from ....connectors.SQL.PostgreSQL.psql_connector import PostgresConnector


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 10, 9, tz='UTC'),
    catchup=False,
    tags=["example"],
)
def oltp_simulation_api():
    @task
    def connect_to_db():
        fk_db_cred = {
            "DB_HOST": "localhost",
            "DB_NAME": "fakestream",
            "DB_USER": "testadmin",
            "DB_PASSWORD": "password",
            "PORT": 55432,
            "SSL_MODE": "disable"
        }
        config = PostgresConfig(
            host=fk_db_cred["DB_HOST"], 
            database=fk_db_cred['DB_NAME'], 
            username=fk_db_cred["DB_USER"],
            password=fk_db_cred["DB_PASSWORD"],
            port=fk_db_cred['PORT'],
            ssl_mode=fk_db_cred["SSL_MODE"]
        )
        connector = PostgresConnector(config=config)
        cnn_str = config.get_connection_string()
        print("="*30)
        print("Connecting to " + cnn_str)
        print(connector.connect())
        print("="*30)
        print("Connected")
        print(f"Database info\n{connector.get_database_info()}")
        print("="*30)
        
        print("Creating schemas inside database")
        print("="*30)
        schema_loader = SchemaLoader(connector.engine)
        print(f"Finished creating\nDatabase info\n{connector.get_database_info()}")
        print("="*30)
        return connector
    
    @task
    def load_to_db(connector: PostgresConnector):
        f = Faker()
        Faker.seed()
        random.seed()
        """
        customers
        Column("customer_id", Integer, primary_key=True),
                Column("full_name", String(100), nullable=False),
                Column("email", String(100), nullable=False, unique=True, index=True),
                Column("phone", String(100)),
                Column("address", Text),
                Column("date_of_birth", Date),
                Column("created_at", DateTime, server_default=func.now(), nullable=False),
                Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now(), nullable=False),
        """
        customers = []
        used_emails = set()
        for id in range(100):
            email = f.email()
            while email in used_emails:
                email = f.email()
            used_emails.add(email)
            customers.append({
                    "fullname": f.name(),
                    "email": email,
                    "phone": f.phone_number(),
                    "adrs": f.address(),
                    "dob": f.date_of_birth()
                })
        connector.execute_many(
            """
            INSERT INTO customers (full_name, email, phone, address, date_of_birth)
            VALUES (:fullname, :email, :phone, :adrs, :dob)
            """,
            customers
        )
        print(f"✅ Inserted {len(customers)} customers")


        # seeding accounts
        """
        accounts
        Table(
            "accounts",
            self.metadata_obj,
            Column("account_id", Integer, primary_key=True),
            Column("customer_id", Integer, ForeignKey("customers.customer_id"), nullable=False),
            Column("account_number", String(20), unique=True, nullable=False, index=True),
            Column("account_type", String(50), nullable=False),  # e.g. savings, checking
            Column("balance", Numeric(15, 2), nullable=False, default=0.00),
            Column("currency", String(10), nullable=False, default="USD"),
            Column("status", String(20), nullable=False, default="active"),
            Column("opened_at", DateTime, server_default=func.now(), nullable=False),
            Column("closed_at", DateTime, nullable=True),
        )

        """
        accounts = []
        account_types = ["Assets", "Customers", "Vendors", "Materials", "General Ledger"]
        ss = ['active', 'inactive', 'blacklisted', 'vip']
        for i in range(50):
            customer = random.choice(customers)
            accounts.append(
                {
                    "customer_id" : customer["cust_id"], # foreign key
                    "account_number" :  f"ACC-{i+1:04d}",
                    "account_type" : random.choice(account_types),
                    "balance" : round(random.uniform(100.0, 10000.0), 2),
                    "currency" : "USD",
                    "status" : random.choice(ss)
                    
                }
            )
        connector.execute_many(
            """
            INSERT INTO accounts (customer_id, account_number, account_type, balance, currency, status)
            VALUES (:customer_id, :account_number, :account_type, :balance, :currency, :status)
            """,
            accounts
        )
        print(f"✅ Inserted {len(accounts)} accounts")
        # seeding transactions
        """
        transactions
        Table(
            "transactions",
            self.metadata_obj,
            Column("transaction_id", Integer, primary_key=True),
            Column("account_id", Integer, ForeignKey("accounts.account_id"), nullable=False),
            Column("transaction_type", String(20), nullable=False),  # deposit, withdrawal, transfer
            Column("amount", Numeric(15, 2), nullable=False),
            Column("currency", String(10), nullable=False, default="USD"),
            Column("transaction_date", DateTime, server_default=func.now(), nullable=False),
            Column("description", Text),
            Column("related_account_id", Integer, ForeignKey("accounts.account_id"), nullable=True),
            Column("status", String(20), nullable=False, default="completed"),
            Column("created_at", DateTime, server_default=func.now(), nullable=False),
        )
        """
        trx = []
        trx_types = ["Deposit", "Withdrawal", "Transfer", "Payment", "Receipt"]
        sts = ["Completed", "Pending", "Blocked"]
        for i in range(2000):
            account1 = random.choice(accounts)
            account2 = random.choice([a for a in accounts if a["account_id"] != account1["account_id"]])
            trx.append({
                "acc_id": account1["account_id"],
                "trx_type": random.choice(trx_types),
                "amt": round(random.uniform(10, 5000), 2),
                "currency": "USD",
                "description": f.sentence(nb_words=3)[:50],
                "related_acc_id": account2["account_id"],
                "status": random.choice(sts)
            })
        
        connector.execute_many(
            """
            INSERT INTO transactions (account_id, transaction_type, amount, currency, description, related_account_id, status)
            VALUES (:acc_id, :trx_type, :amt, :currency, :description, :related_acc_id, :status)
            """,
            trx
        )
        print(f"Inserted {len(trx)} rows")
            
    db_connector = connect_to_db()
    load_to_db(db_connector)
    
oltp_simulation_api()
