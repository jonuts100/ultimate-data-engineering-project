
from ..SQL.PostgreSQL.psql_config import PostgresConfig
from ..SQL.PostgreSQL.psql_connector import PostgresConnector
from generator.data.schemas import SchemaLoader
from sqlalchemy import text
from faker import Faker
from random import randint, randrange
import random
# This file is responsible for the seeding of the fake database with faker
# schema is from schema.py
    
if __name__ == '__main__':
    # host = input("DATABASE HOST: ")
    # name = input("DATABASE NAME: ")
    # user = input("DATABASE USER: ")
    # pw = input("DATABASE PASSWORD: ")
    # port = input("DATABASE PORT: ")
    # sslmode = input("SSL mode [disable/require]: ") or "require"

    # USER_DB_CRED = {
    #     "DB_HOST": host,
    #     "DB_NAME": name,
    #     "DB_USER": user,
    #     "DB_PASSWORD": pw,
    #     "PORT": port,
    #     "SSL_MODE": sslmode
    # }
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
    # print("Table schemas")
    # print(table_schema.items())
    
    # initial seeding using faker
    query = """
            DROP TABLE IF EXISTS transactions, accounts, customers CASCADE;
            """
    connector.execute(query)
    print("="*30)
    print(f"After dropping, Database info\n{connector.get_database_info()}")
    print("="*30)
    
    print("Creating schemas inside database")
    print("="*30)
    schema_loader = SchemaLoader(connector.engine)
    table_schema = schema_loader.tables
    print(f"Finished creating\nDatabase info\n{connector.get_database_info()}")
    print("="*30)
    
    
    f = Faker()
    Faker.seed(42)
    random.seed(42)
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
                "cust_id": id+1,
                "fullname": f.name(),
                "email": email,
                "phone": f.phone_number(),
                "adrs": f.address(),
                "dob": f.date_of_birth()
            })
    connector.execute_many(
        """
        INSERT INTO customers (customer_id, full_name, email, phone, address, date_of_birth)
        VALUES (:cust_id, :fullname, :email, :phone, :adrs, :dob)
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
                "account_id" : i+1,
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
        INSERT INTO accounts (account_id, customer_id, account_number, account_type, balance, currency, status)
        VALUES (:account_id, :customer_id, :account_number, :account_type, :balance, :currency, :status)
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
            "trx_id": i+1,
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
        INSERT INTO transactions (transaction_id, account_id, transaction_type, amount, currency, description, related_account_id, status)
        VALUES (:trx_id, :acc_id, :trx_type, :amt, :currency, :description, :related_acc_id, :status)
        """,
        trx
    )
    print(f"Inserted {len(trx)} rows")
        