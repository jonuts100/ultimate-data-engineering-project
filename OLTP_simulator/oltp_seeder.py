# Dynamically add project root to Python path
import os
import sys

PROJECT_ROOT = "/home/jonut/Projects/ultimate-data-engineering-project"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
import airflow
from airflow.sdk import dag, task
import pendulum
from faker import Faker
import random
from datetime import timedelta
from OLTP_simulator.generator.data.schemas import SchemaLoader
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_config import PostgresConfig
from OLTP_simulator.connectors.SQL.PostgreSQL.psql_connector import PostgresConnector


@dag(
    dag_id="oltp_simulator_dag",
    schedule="@hourly",
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
    
    DATA QUALITY ISSUES SIMULATED:
    1. Duplicate Customers (name variations, typos in email)
    2. Missing/Null Values (phone, address incomplete)
    3. Late-Arriving Transactions (arrive days after creation)
    4. Future-Dated Records (data entry errors)
    5. Impossible Values (negative ages, balances)
    6. Circular Transfers (A→B→A same day, fraud indicator)
    7. Inactive Account Transactions (processing on closed accounts)
    8. Slowly Changing Dimensions (address changes, status updates)
    9. Data Type Mismatches (text in numeric fields)
    10. Referential Integrity Issues (orphaned records)
    
    These issues make the transformation layer meaningful and realistic.
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
        """
        Generate customers with intentional data quality issues:
        - Duplicate customers (same person, different spellings)
        - Missing/null values (30% missing phone, 20% missing address)
        - Data entry errors (typos in emails)
        - Invalid data (impossible birth dates)
        """
        
        # CANT pass large stuff like PostgresConnector
        # So just rebuild connector at each step
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        f = Faker()
        now = pendulum.now()
        
        # Vary daily volume based on day of week
        base_customers = 10000
        if now.day_of_week in [5, 6]:  # Weekend -> fewer signups
            num_customers = random.randint(2000, 5000)
        else:  # Weekday
            num_customers = random.randint(base_customers - 20, base_customers + 30)
        
        print(f"\n👥 Onboarding {num_customers} new customers...")
        
        # Get existing emails
        result = connector.execute("SELECT email FROM customers")
        existing_emails = {row[0] for row in result}
        
        customers_data = []
        duplicate_cust_names = []
        attempts = 0
        while len(customers_data) < num_customers and attempts < num_customers * 5:
            
            # QUALITY ISSUE #1 -> DUPLICATE CUSTOMERS
            # SIMULATE 8% CHANCE OF CREATING A NEAR-DUPLIACTE CUSTOMER
            # EX. SAME NAME, DIFFERENT EMAIL (INVALID)
            
            email = f.email()
            if random.random() < 0.08 and duplicate_cust_names:
                fullname = random.choice(duplicate_cust_names)
                email = f.email().replace('@gmail.com', '@binus.ac.id')
            else:
                fullname = f.name()
                duplicate_cust_names.append(fullname)
            
            if email not in existing_emails:
                # QUALITY ISSUE #2: Missing/Null Values (30% no phone, 20% no address)
                phone = f.phone_number() if random.random() <= 0.70 else None
                addrs = f.address().replace('\n', ',') if random.random() <= 0.8 else None
                # QUALITY ISSUE #3: Data Entry Errors (5% typo in email)
                email = email.replace('a', '4').replace('e', '3') if random.random() < 0.05 else email
                # QUALITY ISSUE #4: Impossible Birth Dates (1% impossible birth rate or data entry error)
                dob = f.date_of_birth(minimum_age=1, maximum_age=5) if random.random() <= 0.01 else f.date_of_birth(minimum_age=18, maximum_age=80)
                customers_data.append({
                    "fullname": fullname,
                    "email": email,
                    "phone": phone,
                    "adrs": addrs,
                    "dob": dob
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
            print(f"   📌 Quality Issues: ~{int(len(customers_data)*0.08)} duplicates, "
                  f"~{int(len(customers_data)*0.30)} missing phone, "
                  f"~{int(len(customers_data)*0.20)} missing address")
        return len(customers_data)
    
    # @task
    # def simulate_customer_updates(DB_CONFIG: dict):
    #     """
    #     Simulate customer updates creating Slowly Changing Dimension issues.
        
    #     Example: Customer moves, changes phone, etc.
    #     This creates the need for SCD Type 2 handling in transformation layer.
    #     """
    #     f = Faker()
    #     connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        
    #     # Get random active customers (10% update their info daily)
    #     result = connector.execute("""
    #         SELECT * FROM customers 
    #         ORDER BY RANDOM() 
    #         LIMIT (SELECT CAST(COUNT(*) * 0.10 AS INTEGER) FROM customers)
    #     """)
        
    #     customers_to_update = [row for row in result]
        
    #     if not customers_to_update:
    #         print("⚠️ No customers to update")
    #         return 0
        
    #     print(f"\n📝 Updating {len(customers_to_update)} customer records...")
        
        
    #     for cust in customers_to_update:
            
    #         # cust structure: list -> [customer_id, full_name, email, phone, address, date_of_birth, created_at, updated_at]
    #         cid = cust[0]
    #         updated_cust = list(cust)
    #         print(updated_cust, type(updated_cust))
    #         update_type = random.choice(['phone', 'address', 'both', 'none'])
    #         # Changed phone number or inactive number
    #         if update_type in ['phone', 'both']:
    #             phone = f.phone_number() if random.random() > 0.2 else None
    #             # phone is index 3
    #             updated_cust[3] = phone
                
    #         # Changed location to somewhere or hiding lol
    #         if update_type in ['address', 'both']:
    #             address = f.address().replace('\n', ', ') if random.random() > 0.15 else None
    #             updated_cust[4] = address

    #         updated_cust_dict = {
    #             "fullname": updated_cust[1],
    #             "email": updated_cust[2],
    #             "phone": updated_cust[3],
    #             "adrs": updated_cust[4],
    #             "dob": updated_cust[5]
    #         }
    #         # Update the updated_at column of original customer
    #         connector.execute(
    #             """
    #             UPDATE customers 
    #             SET updated_at = NOW() 
    #             WHERE customer_id = :customer_id
    #             """,
    #             {"customer_id": cid}
    #         )
            
    #         # Insert new row for the changes made
    #         connector.execute(
    #             """
    #             INSERT INTO customers (full_name, email, phone, address, date_of_birth)
    #             VALUES (:fullname, :email, :phone, :adrs, :dob)
    #             """,
    #             updated_cust_dict
    #         )
            
    #     print(f"✅ Updated {len(customers_to_update)} customer records")
    #     print(f"   📌 Tracking these as SCD Type 2 changes in transformation layer")
    #     return len(customers_to_update)
    
    @task
    def simulate_account_creation(DB_CONFIG: dict):
        """
        Generate accounts with data quality issues:
        - Impossible balances (negative)
        - Status inconsistencies
        - Duplicate account numbers
        """
        
        # Get customers who might open new accounts (recent customers + random existing)
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
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
        
        num_accounts = random.randint(1000, 3000)
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
            # QUALITY ISSUE #5: Impossible Balance Values
            # 2% negative balance (accounting error)
            if random.random() < 0.02:
                balance = round(random.uniform(-5000.0, -100.0), 2)
            else:
                balance = round(random.uniform(50.0, 100000.0), 2)
            
            accounts_data.append({
                "customer_id": random.choice(eligible_customers),
                "account_number": account_num,
                "account_type": random.choice(account_types),
                "balance": balance,
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
        print(f"   📌 Quality Issues: ~{int(len(accounts_data)*0.02)} negative balances")
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
        Generate transactions with realistic data quality issues:
        - Late-arriving transactions (arrive 5-30 days late)
        - Future-dated transactions (data entry errors)
        - Transactions on closed accounts
        - Circular transfers (fraud pattern)
        - Inactive account transactions
        - Status inconsistencies (failed but balance updated)
        """
        
        connector = PostgresConnector(config=PostgresConfig(**DB_CONFIG))
        f = Faker()
        now = pendulum.now()
        
        # Get active accounts
        result = connector.execute("""
            SELECT account_id, balance, account_type, status
            FROM accounts 
            WHERE status = 'active'
        """)
        accounts = [{"id": row[0], "balance": float(row[1]), "type": row[2], "status": row[3]} for row in result]
        active_accounts = [a for a in accounts if a["status"] == "active"]
        inactive_accounts = [a for a in accounts if a["status"] in ["inactive", "suspended"]]
        
        if len(active_accounts) < 2:
            print("⚠️ Not enough active accounts")
            return 0
        
        # Volume varies by day and hour
        base_volume = len(active_accounts) * 10  # 10 transactions per account avg
        
        # Weekend: 30% less activity
        if now.day_of_week in [5, 6]:
            base_volume = int(base_volume * 0.7)
        
        # Month end: 50% more activity
        if now.day >= 28:
            base_volume = int(base_volume * 1.5)
        
        num_transactions = random.randint(int(base_volume * 0.8), int(base_volume * 1.2))
        print(f"\n💰 Processing {num_transactions} transactions...")
        
        trx_distribution = {
            "Deposit": 0.35,      # 35% deposits
            "Withdrawal": 0.30,   # 30% withdrawals
            "Transfer": 0.25,     # 25% transfers
            "Payment": 0.08,      # 8% payments
            "Refund": 0.02        # 2% refunds
        }
        
        statuses_weighted = ['completed'] * 90 + ['pending'] * 7 + ['failed'] * 3
        transactions_data = []
        balance_updates = {}
        recent_transfers = {}
        
        quality_issue_counts = {
            'late_arriving': 0,
            'future_dated': 0,
            'inactive_account': 0,
            'circular_transfer': 0,
            'impossible_amount': 0
        }
        
        for _ in range(num_transactions):
            # QUALITY ISSUE #6: Transactions on Inactive/Closed Accounts
            # 3% of transactions happen on inactive accounts (shouldn't happen in real system)
            if random.random() < 0.03 and inactive_accounts:
                account = random.choice(inactive_accounts)
                quality_issue_counts['inactive_account'] += 1
            else:
                account = random.choice(active_accounts)
            
            trx_type = random.choices(list(trx_distribution.keys()), weights=list(trx_distribution.values()))[0]
            
            status = random.choice(statuses_weighted)
            
            # QUALITY ISSUE #5: Impossible Transaction Amounts
            if random.random() < 0.01:  # 1% impossible amounts
                if trx_type == "Withdrawal":
                    amount = round(random.uniform(10000, 99999), 2)  # Overdraft
                else:
                    amount = round(random.uniform(100000, 999999), 2)  # Unrealistic
                quality_issue_counts['impossible_amount'] += 1
            else:
                if trx_type == "Deposit":
                    amount = round(random.uniform(50, 5000), 2)
                elif trx_type == "Withdrawal":
                    amount = round(random.uniform(20, min(account["balance"] * 0.5, 1000)), 2)
                elif trx_type == "Transfer":
                    amount = round(random.uniform(10, min(account["balance"] * 0.5, 2000)), 2)
                elif trx_type == "Payment":
                    amount = round(random.uniform(5, 500), 2)
                else:
                    amount = round(random.uniform(10, 200), 2)
            
            related_acc = None
            # QUALITY ISSUE #7: Circular Transfers (A→B→A, fraud indicator)
            if trx_type == "Transfer":
                related_acc = random.choice([a["id"] for a in accounts if a["id"] != account["id"]])

                if random.random() < 0.02:
                    if account['id'] in recent_transfers:
                        related_acc = recent_transfers[account["id"]]
                        quality_issue_counts['circular_transfer'] += 1
                recent_transfers[account["id"]] = related_acc
            
            # QUALITY ISSUE #3: Late-Arriving Transactions
            # 5% of transactions arrive 5-30 days late
            transaction_date = now
            if random.random() < 0.05:
                days_late = random.randint(5, 30)
                transaction_date = now - timedelta(days=days_late)
                quality_issue_counts['late_arriving'] += 1
           
            
            transactions_data.append({
                "acc_id": account["id"],
                "trx_type": trx_type,
                "amt": amount,
                "currency": "USD",
                "description": f.sentence(nb_words=4)[:100],
                "related_acc_id": related_acc,
                "status": status,
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
        print(f"   📌 Quality Issues Found:")
        print(f"      • Late-arriving (5-30 days): {quality_issue_counts['late_arriving']}")
        print(f"      • Future-dated: {quality_issue_counts['future_dated']}")
        print(f"      • Inactive account transactions: {quality_issue_counts['inactive_account']}")
        print(f"      • Circular transfers (fraud pattern): {quality_issue_counts['circular_transfer']}")
        print(f"      • Impossible amounts: {quality_issue_counts['impossible_amount']}")
        print(f"   ✅ These will be caught in transformation layer!")
        
        return len(transactions_data)
    
    
    @task
    def generate_daily_report(
                            DB_CONFIG: dict,
                            new_customers: int, 
                            # updated_customers: int,
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
        # print(f"  • Customer Updates: {updated_customers}")
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
            return "Hourly report created."
        except Exception:
            pass
    
    # DAG Workflow - simulates realistic OLTP operations
    db_conn = connect_to_db()
    
    # Customer lifecycle
    new_custs = simulate_customer_onboarding(db_conn)
    # updated_custs = simulate_customer_updates(db_conn)
    
    # # Account lifecycle
    new_accts = simulate_account_creation(db_conn)
    closed_accts = simulate_account_closure(db_conn)
    
    # Transaction processing (most frequent operation)
    trx_count = simulate_transactions(db_conn)
    
    # Daily summary
    report = generate_daily_report(db_conn, new_custs, new_accts, closed_accts, trx_count)
    # make it ordered
    db_conn >> new_custs >> [new_accts, closed_accts] >> trx_count >> report

dag_instance = realistic_oltp_simulation()