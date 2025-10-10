
from ..SQL.PostgreSQL.psql_config import PostgresConfig
from ..SQL.PostgreSQL.psql_connector import PostgresConnector
from generator.data.schemas import SchemaLoader
from faker import Faker
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
    try:
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
        print("📊 Generating customers...")
        print("="*50)
        
        customers_data = []
        existing_emails = set()
        
        try:
            result = connector.fetch("""
                                    SELECT email FROM customers
                                    """)
            existing_emails = {row[0] for row in result}
            print(f"Found {len(existing_emails)} existing email in database.")
        except Exception as e:
            print(f"No existing customers found: {e}")
        
        num_account_created = 0
        max_attempts = 500
        daily_account_created = 10000
        attempts = 0
        
        while num_account_created < daily_account_created and attempts < max_attempts:
            current_email = f.email()
            if current_email not in existing_emails:
                customers_data.append({
                    "full_name": f.name(),
                    "email": current_email,
                    "phone": f.phone_number(),
                    "adrs": f.address().replace('\n', ', '),
                    "dob": f.date_of_birth(minimum_age=18, maximum_age=80)
                })
            attempts += 1
            num_account_created += 1
            existing_emails.add(current_email)
        
        if len(customers_data) == 0:
            print("⚠️ No new unique customers to insert")
        else:
            connector.execute_many(
                """
                INSERT INTO customers (full_name, email,phone, address,date_of_birth)
                VALUES (:full_name, :email, :phone, :adrs, :dob)
                """,
                customers_data
            )
        
        print(f"✅ Inserted {len(customers_data)} customers")
                
        # Fetch inserted customer IDs for foreign key references
        result = connector.execute(
            "SELECT customer_id FROM customers ORDER BY created_at DESC LIMIT :limit",
            {"limit": len(customers_data)}
        )
        customer_ids = [row[0] for row in result]
        # ============================================
        # ACCOUNTS
        # ============================================
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
        print("\n📊 Generating accounts...")
        accounts_data = []
        account_types = ["Savings", "Checking", "Investment", "Credit", "Loan"]
        statuses = ['active', 'inactive', 'suspended', 'closed']

        existing_account_nums = set()
        try:
            result = connector.fetch("SELECT account_number FROM accounts")
            existing_account_nums = {row[0] for row in result}
        except Exception as e:
            pass
        
        account_counter = len(existing_account_nums) + 1
        num_accounts = min(len(customer_ids) * 2, 1000)
        for i in range(num_accounts):
            account_num = f"ACC-{account_counter:06d}"
            while account_num in existing_account_nums:
                account_counter += 1
                account_num = f"ACC-{account_counter:06d}"
            
            customer_id = random.choice(customer_ids)
            accounts_data.append(
                {
                    "customer_id" : customer_id, # foreign key
                    "account_number" :  account_num,
                    "account_type" : random.choice(account_types),
                    "balance" : round(random.uniform(100.0, 1000000.0), 2),
                    "currency" : "USD",
                    "status" : random.choice(statuses)
                }
            )
            existing_account_nums.add(account_num)
            account_counter += 1
            
        # inserting to db
        connector.execute_many(
            """
            INSERT INTO accounts (customer_id, account_number, account_type, balance, currency, status)
            VALUES (:customer_id, :account_number, :account_type, :balance, :currency, :status)
            """,
            accounts_data
        )
        print(f"✅ Inserted {len(accounts_data)} accounts")
        
        # Fetch inserted account IDs for transactions
        result = connector.execute("""
                                SELECT account_id FROM accounts ORDER BY opened_at DESC LIMIT :limit
                                """,
                                {"limit": len(accounts_data)}
                                )
        account_ids = [row[0] for row in result]
        if len(account_ids) < 2:
            print("⚠️ Not enough accounts for transactions")
            
        
        # ============================================
        # TRANSACTIONS
        # ============================================
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
        EXPECTED_TRANSACTIONS = 10000
        trx_data = []
        trx_types = ["Deposit", "Withdrawal", "Transfer", "Payment", "Receipt"]
        sts = ["Completed", "Pending", "Blocked"]
        for _ in range(EXPECTED_TRANSACTIONS):
            acc_id = random.choice(account_ids)
            related_acc_id = random.choice([a for a in account_ids if a != acc_id])
            trx_data.append({
                "acc_id": acc_id,
                "trx_type": random.choice(trx_types),
                "amt": round(random.uniform(10, 5000), 2),
                "currency": "USD",
                "description": f.sentence(nb_words=3)[:50],
                "related_acc_id": related_acc_id,
                "status": random.choice(sts)
            })
        
        connector.execute_many(
            """
            INSERT INTO transactions (account_id, transaction_type, amount, currency, description, related_account_id, status)
            VALUES (:acc_id, :trx_type, :amt, :currency, :description, :related_acc_id, :status)
            """,
            trx_data
        )
        print(f"✅ Inserted {len(trx_data)} rows")
        
        # Summary
        print("\n" + "="*50)
        print("📈 DATA LOAD SUMMARY:")
        print(f"  • Customers: {len(customers_data)}")
        print(f"  • Accounts: {len(accounts_data)}")
        print(f"  • Transactions: {len(trx_data)}")
        print("="*50)
    except Exception as e:
        print(f"❌ Data load failed: {e}")
        raise
    finally:
        # Close connection
        try:
            connector.close()
            print("🔌 Connection closed")
        except Exception:
            pass