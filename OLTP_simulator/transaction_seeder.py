import airflow
from airflow.sdk import dag, task
import pendulum
from faker import Faker
import random
from datetime import datetime, timedelta
from OLTP_simulator.generator.data.schemas import SchemaLoader
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_config import PostgresConfig
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_connector import PostgresConnector


@dag(
    dag_id="oltp_simulator_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 10, 9, tz='UTC'),
    catchup=False,
    tags=["oltp", "simulation"],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def realistic_oltp_simulation():
    """
    Simulates a realistic OLTP banking system with:
    - New customer onboarding
    - Account lifecycle (open/close/update)
    - Transaction processing with balance updates
    - Customer updates (address, phone changes)
    - Temporal patterns (business hours, weekends)
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
            print("✅ Database connection established")
            
            return DB_CONFIG
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
    
    @task
    def simulate_customer_onboarding(DB_CONFIG: dict):
        """Add new customers (simulates new account signups)"""
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        f = Faker()
        now = pendulum.now()
        
        # Vary daily volume based on day of week
        base_customers = 1000
        if now.day_of_week in [5, 6]:  # Weekend - fewer signups
            num_customers = random.randint(100, 300)
        else:  # Weekday
            num_customers = random.randint(base_customers - 20, base_customers + 30)
        
        print(f"\n👥 Onboarding {num_customers} new customers...")
        
        # Get existing emails
        result = connector.execute("SELECT email FROM customers")
        existing_emails = {row[0] for row in result}
        
        customers_data = []
        attempts = 0
        while len(customers_data) < num_customers and attempts < num_customers * 5:
            email = f.email()
            if email not in existing_emails:
                customers_data.append({
                    "fullname": f.name(),
                    "email": email,
                    "phone": f.phone_number(),
                    "adrs": f.address().replace('\n', ', '),
                    "dob": f.date_of_birth(minimum_age=18, maximum_age=75)
                })
                existing_emails.add(email)
            attempts += 1
        
        if customers_data:
            connector.execute_many(
                """
                INSERT INTO customers (full_name, email, phone, address, date_of_birth)
                VALUES (:fullname, :email, :phone, :adrs, :dob)
                """,
                customers_data
            )
            print(f"✅ Onboarded {len(customers_data)} customers")
        
        return len(customers_data)
    
    @task
    def simulate_customer_updates(DB_CONFIG: dict):
        """Update existing customer information (address changes, phone updates)"""
        f = Faker()
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        # Get random active customers (5% update their info daily)
        result = connector.execute("""
            SELECT customer_id FROM customers 
            ORDER BY RANDOM() 
            LIMIT (SELECT CAST(COUNT(*) * 0.05 AS INTEGER) FROM customers)
        """)
        customer_ids = [row[0] for row in result]
        
        if not customer_ids:
            print("⚠️ No customers to update")
            return 0
        
        print(f"\n📝 Updating {len(customer_ids)} customer records...")
        
        updates = []
        for cid in customer_ids:
            update_type = random.choice(['phone', 'address', 'both'])
            update_data = {"customer_id": cid}
            
            if update_type in ['phone', 'both']:
                update_data['phone'] = f.phone_number()
            if update_type in ['address', 'both']:
                update_data['address'] = f.address().replace('\n', ', ')
        
            if 'phone' in update_data and 'address' in update_data:
                connector.execute(
                    "UPDATE customers SET phone = :phone, address = :address WHERE customer_id = :customer_id",
                    update_data
                )
            elif 'phone' in update_data:
                connector.execute(
                    "UPDATE customers SET phone = :phone WHERE customer_id = :customer_id",
                    update_data
                )
            elif 'address' in update_data:
                connector.execute(
                    "UPDATE customers SET address = :address WHERE customer_id = :customer_id",
                    update_data
                )
        
        print(f"✅ Updated {len(customer_ids)} customer records")
        return len(customer_ids)
    
    @task
    def simulate_account_creation(DB_CONFIG: dict):
        """Create new accounts for existing customers"""
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        # Get customers who might open new accounts (recent customers + random existing)
        result = connector.execute("""
            SELECT customer_id FROM customers 
            WHERE created_at > NOW() - INTERVAL '3 days'
            OR customer_id IN (
                SELECT customer_id FROM customers 
                ORDER BY RANDOM() 
                LIMIT 20
            )
        """)
        eligible_customers = [row[0] for row in result]
        
        if not eligible_customers:
            print("⚠️ No eligible customers for new accounts")
            return 0
        
        # Get existing account numbers
        result = connector.execute("SELECT account_number FROM accounts")
        existing_nums = {row[0] for row in result}
        
        num_accounts = random.randint(20, 50)
        print(f"\n🏦 Creating {num_accounts} new accounts...")
        
        account_types = ["Savings", "Checking", "Investment", "Credit"]
        statuses = ['active'] * 85 + ['inactive'] * 10 + ['suspended'] * 5  # Weighted distribution
        
        accounts_data = []
        counter = len(existing_nums) + 1
        
        for _ in range(num_accounts):
            account_num = f"ACC-{counter:08d}"
            while account_num in existing_nums:
                counter += 1
                account_num = f"ACC-{counter:08d}"
            
            accounts_data.append({
                "customer_id": random.choice(eligible_customers),
                "account_number": account_num,
                "account_type": random.choice(account_types),
                "balance": round(random.uniform(100.0, 10000.0), 2),
                "currency": "USD",
                "status": random.choice(statuses)
            })
            existing_nums.add(account_num)
            counter += 1
        
        connector.execute_many(
            """
            INSERT INTO accounts (customer_id, account_number, account_type, balance, currency, status)
            VALUES (:customer_id, :account_number, :account_type, :balance, :currency, :status)
            """,
            accounts_data
        )
        print(f"✅ Created {len(accounts_data)} accounts")
        return len(accounts_data)
    
    @task
    def simulate_account_closure(DB_CONFIG: dict):
        """Close some accounts (realistic account lifecycle)"""
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        # Close 1-2% of active accounts randomly
        result = connector.execute("""
            SELECT account_id FROM accounts 
            WHERE status = 'active' 
            ORDER BY RANDOM() 
            LIMIT (SELECT CAST(COUNT(*) * 0.02 AS INTEGER) FROM accounts WHERE status = 'active')
        """)
        accounts_to_close = [row[0] for row in result]
        
        if not accounts_to_close:
            return 0
        
        print(f"\n🔒 Closing {len(accounts_to_close)} accounts...")
        
        for acc_id in accounts_to_close:
            connector.execute(
                """
                UPDATE accounts 
                SET status = 'closed', closed_at = NOW() 
                WHERE account_id = :account_id
                """,
                {"account_id": acc_id}
            )
        
        print(f"✅ Closed {len(accounts_to_close)} accounts")
        return len(accounts_to_close)
    
    @task
    def simulate_transactions(DB_CONFIG: dict):
        """
        Generate realistic transaction patterns with balance updates.
        Simulates: deposits, withdrawals, transfers between accounts.
        """
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        f = Faker()
        now = pendulum.now()
        
        # Get active accounts
        result = connector.execute("""
            SELECT account_id, balance, account_type 
            FROM accounts 
            WHERE status = 'active'
        """)
        accounts = [{"id": row[0], "balance": float(row[1]), "type": row[2]} for row in result]
        
        if len(accounts) < 2:
            print("⚠️ Not enough active accounts for transactions")
            return 0
        
        # Volume varies by day and hour
        base_volume = len(accounts) * 15  # 15 transactions per account avg
        
        # Weekend: 30% less activity
        if now.day_of_week in [5, 6]:
            base_volume = int(base_volume * 0.7)
        
        # Month end: 50% more activity
        if now.day >= 28:
            base_volume = int(base_volume * 1.5)
        
        num_transactions = random.randint(int(base_volume * 0.8), int(base_volume * 1.2))
        print(f"\n💰 Processing {num_transactions} transactions...")
        
        # 80/20 rule: 20% of accounts do 80% of transactions
        # at least 100 transactions but should never be 100 only 
        active_accounts = random.sample(accounts, k=max(100, len(accounts) // 5))
        normal_accounts = [a for a in accounts if a not in active_accounts]
        
        transactions_data = []
        balance_updates = {}
        
        trx_distribution = {
            "Deposit": 0.35,      # 35% deposits
            "Withdrawal": 0.30,   # 30% withdrawals
            "Transfer": 0.25,     # 25% transfers
            "Payment": 0.08,      # 8% payments
            "Refund": 0.02        # 2% refunds
        }
        
        statuses_weighted = ['completed'] * 90 + ['pending'] * 7 + ['failed'] * 3
        
        for _ in range(num_transactions):
            # Use active accounts 80% of the time
            if random.random() < 0.8 and active_accounts:
                account = random.choice(active_accounts)
            else:
                account = random.choice(normal_accounts) if normal_accounts else random.choice(accounts)
            
            trx_type = random.choices(list(trx_distribution.keys()), weights=list(trx_distribution.values()))
            
            status = random.choice(statuses_weighted)
            
            # Amount varies by transaction type
            if trx_type == "Deposit":
                amount = round(random.uniform(10, 5000) , 2)
            elif trx_type == "Withdrawal":
                amount = round(random.uniform(20, min(account["balance"] * 0.3, 1000)), 2)
            elif trx_type == "Transfer":
                amount = round(random.uniform(10, min(account["balance"] * 0.5, 2000)), 2)
            elif trx_type == "Payment":
                amount = round(random.uniform(5, 500), 2)
            else:  # Refund
                amount = round(random.uniform(10, 200), 2)
            
            related_acc = None
            if trx_type == "Transfer":
                related_acc = random.choice([a["id"] for a in accounts if a["id"] != account["id"]])
            
            transactions_data.append({
                "acc_id": account["id"],
                "trx_type": trx_type,
                "amt": amount,
                "currency": "USD",
                "description": f.sentence(nb_words=4)[:50],
                "related_acc_id": related_acc,
                "status": status
            })
            
            # Update balances only for completed transactions
            if status == "completed":
                if account["id"] not in balance_updates:
                    # Update dictionary of balance where key is the user
                    # this simulates the balance of the user searchable by their id
                    balance_updates[account["id"]] = account["balance"]
                
                if trx_type in ["Deposit", "Refund"]:
                    # add money to balance
                    balance_updates[account["id"]] += amount
                elif trx_type in ["Withdrawal", "Payment"]:
                    # User send money so deduct from balance, but balance cannot be 0, so we use max()
                    balance_updates[account["id"]] = max(0, balance_updates[account["id"]] - amount)
                elif trx_type == 'Transfer' and related_acc:
                    # Update 2 balances: the sender and receiver
                    balance_updates[account["id"]] = max(0, balance_updates[account["id"]] - amount)
                    if related_acc not in balance_updates:
                        # find balance of related account, only dictionary we have is from all accounts, maybe can be optimized?
                        related_balance = next(a["balance"] for a in accounts if a["id"] == related_acc)
                        balance_updates[related_acc] = related_balance 
                    balance_updates[related_acc] += amount
        
        # Insert transactions
        connector.execute_many(
            """
            INSERT INTO transactions (account_id, transaction_type, amount, currency, 
                                     description, related_account_id, status)
            VALUES (:acc_id, :trx_type, :amt, :currency, :description, :related_acc_id, :status)
            """,
            transactions_data
        )
        
        # Update account balances
        for acc_id, new_balance in balance_updates.items():
            connector.execute(
                "UPDATE accounts SET balance = :balance WHERE account_id = :account_id",
                {"balance": round(new_balance, 2), "account_id": acc_id}
            )
        
        print(f"✅ Processed {len(transactions_data)} transactions")
        print(f"✅ Updated {len(balance_updates)} account balances")
        return len(transactions_data)
    
    @task
    def generate_daily_report(
                            DB_CONFIG: dict,
                            new_customers: int, 
                            updated_customers: int,
                            new_accounts: int, 
                            closed_accounts: int, 
                            transactions: int
                            ):
        """Generate summary report of daily activity"""
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        # Get current totals
        result = connector.execute("SELECT COUNT(*) FROM customers")
        total_customers = result.fetchone()[0]
        
        result = connector.execute("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
        active_accounts = result.fetchone()[0]
        
        result = connector.execute("SELECT SUM(balance) FROM accounts WHERE status = 'active'")
        total_balance = result.fetchone()[0] or 0
        
        result = connector.execute("""
            SELECT COUNT(*) FROM transactions 
            WHERE DATE(transaction_date) = CURRENT_DATE
        """)
        today_transactions = result.fetchone()[0]
        
        print("\n" + "="*60)
        print("📊 DAILY OLTP SIMULATION REPORT")
        print("="*60)
        print(f"Date: {pendulum.now().to_date_string()}")
        print("\n🆕 NEW ACTIVITY:")
        print(f"  • New Customers: {new_customers}")
        print(f"  • New Accounts: {new_accounts}")
        print(f"  • Transactions: {transactions}")
        print("\n✏️  UPDATES:")
        print(f"  • Customer Updates: {updated_customers}")
        print(f"  • Accounts Closed: {closed_accounts}")
        print("\n📈 CURRENT TOTALS:")
        print(f"  • Total Customers: {total_customers}")
        print(f"  • Active Accounts: {active_accounts}")
        print(f"  • Total Balance: ${total_balance:,.2f}")
        print(f"  • Today's Transactions: {today_transactions}")
        print("="*60)
        
        try:
            connector.close()
            print("🔌 Connection closed")
        except Exception:
            pass
    
    # DAG Workflow - simulates realistic OLTP operations
    db_conn = connect_to_db()
    
    # Customer lifecycle
    new_custs = simulate_customer_onboarding(db_conn)
    updated_custs = simulate_customer_updates(db_conn)
    
    # Account lifecycle
    new_accts = simulate_account_creation(db_conn)
    closed_accts = simulate_account_closure(db_conn)
    
    # Transaction processing (most frequent operation)
    trx_count = simulate_transactions(db_conn)
    
    # Daily summary
    generate_daily_report(db_conn, new_custs, updated_custs, new_accts, closed_accts, trx_count)

dag_instance = realistic_oltp_simulation()