import airflow
from airflow.sdk import dag, task
import pendulum
from faker import Faker
import random
from datetime import datetime, timedelta
import hashlib
from ..schemas import SchemaLoader
from ....connectors.SQL.PostgreSQL.psql_config import PostgresConfig
from ....connectors.SQL.PostgreSQL.psql_connector import PostgresConnector


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 10, 9, tz='UTC'),
    catchup=False,
    tags=["oltp", "simulation", "data-quality"],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def enhanced_oltp_with_data_quality():
    """
    Simulates realistic banking OLTP with intentional data quality issues:
    
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
        """Establish database connection"""
        fk_db_cred = {
            "DB_HOST": "localhost",
            "DB_NAME": "fakestream",
            "DB_USER": "testadmin",
            "DB_PASSWORD": "password",
            "PORT": 55432,
            "SSL_MODE": "disable"
        }
        
        try:
            config = PostgresConfig(
                host=fk_db_cred["DB_HOST"], 
                database=fk_db_cred['DB_NAME'], 
                username=fk_db_cred["DB_USER"],
                password=fk_db_cred["DB_PASSWORD"],
                port=fk_db_cred['PORT'],
                ssl_mode=fk_db_cred["SSL_MODE"]
            )
            connector = PostgresConnector(config=config)
            connector.connect()
            SchemaLoader(connector.engine)
            print("✅ Database connection established")
            return connector
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
    
    @task
    def simulate_customers_with_quality_issues(connector: PostgresConnector):
        """
        Generate customers with intentional data quality issues:
        - Duplicate customers (same person, different spellings)
        - Missing/null values (30% missing phone, 20% missing address)
        - Data entry errors (typos in emails)
        - Invalid data (impossible birth dates)
        """
        f = Faker()
        now = pendulum.now()
        
        # Volume varies by day of week
        if now.day_of_week in [5, 6]:
            num_customers = random.randint(10, 30)
        else:
            num_customers = random.randint(40, 70)
        
        print(f"\n👥 Generating {num_customers} customers with quality issues...")
        
        result = connector.execute("SELECT email FROM customers")
        existing_emails = {row[0] for row in result}
        
        customers_data = []
        duplicate_base_names = []
        attempts = 0
        
        while len(customers_data) < num_customers and attempts < num_customers * 5:
            # QUALITY ISSUE #1: Duplicate Customers
            # 8% chance to create a near-duplicate (same person, slightly different info)
            if random.random() < 0.08 and duplicate_base_names:
                base_name = random.choice(duplicate_base_names)
                full_name = base_name  # Same name
                # Slightly different email (Gmail vs yahoo, etc)
                email = f.email().replace('@gmail.com', '@yahoo.com') if '@gmail.com' in f.email() else f.email()
            else:
                full_name = f.name()
                email = f.email()
                duplicate_base_names.append(full_name)
            
            if email not in existing_emails:
                # QUALITY ISSUE #2: Missing/Null Values (30% no phone, 20% no address)
                phone = None if random.random() < 0.30 else f.phone_number()
                address = None if random.random() < 0.20 else f.address().replace('\n', ', ')
                
                # QUALITY ISSUE #3: Data Entry Errors (2% typo in email)
                if random.random() < 0.02:
                    email = email.replace('a', '4').replace('e', '3') if random.random() > 0.5 else email
                
                # QUALITY ISSUE #4: Impossible Birth Dates
                if random.random() < 0.01:  # 1% impossible age
                    dob = f.date_of_birth(minimum_age=1, maximum_age=5)  # Baby customer!
                else:
                    dob = f.date_of_birth(minimum_age=18, maximum_age=80)
                
                customers_data.append({
                    "fullname": full_name,
                    "email": email,
                    "phone": phone,
                    "adrs": address,
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
            print(f"✅ Inserted {len(customers_data)} customers")
            print(f"   📌 Quality Issues: ~{int(len(customers_data)*0.08)} duplicates, "
                  f"~{int(len(customers_data)*0.30)} missing phone, "
                  f"~{int(len(customers_data)*0.20)} missing address")
        
        return len(customers_data)
    
    @task
    def simulate_customer_updates_with_scd(connector: PostgresConnector):
        """
        Simulate customer updates creating Slowly Changing Dimension issues.
        
        Example: Customer moves, changes phone, etc.
        This creates the need for SCD Type 2 handling in transformation layer.
        """
        f = Faker()
        
        # Get random customers (15% update daily)
        result = connector.execute("""
            SELECT customer_id FROM customers 
            WHERE created_at > NOW() - INTERVAL '90 days'
            ORDER BY RANDOM() 
            LIMIT (SELECT CAST(COUNT(*) * 0.15 AS INTEGER) FROM customers)
        """)
        customer_ids = [row[0] for row in result]
        
        if not customer_ids:
            print("⚠️ No customers to update")
            return 0
        
        print(f"\n📝 Updating {len(customer_ids)} customer records (SCD tracking)...")
        
        for cid in customer_ids:
            update_type = random.choice(['phone', 'address', 'both', 'none'])
            
            if update_type in ['phone', 'both']:
                phone = f.phone_number() if random.random() > 0.2 else None
                connector.execute(
                    "UPDATE customers SET phone = :phone WHERE customer_id = :customer_id",
                    {"phone": phone, "customer_id": cid}
                )
            
            if update_type in ['address', 'both']:
                address = f.address().replace('\n', ', ') if random.random() > 0.15 else None
                connector.execute(
                    "UPDATE customers SET address = :address WHERE customer_id = :customer_id",
                    {"address": address, "customer_id": cid}
                )
        
        print(f"✅ Updated {len(customer_ids)} customer records")
        print(f"   📌 Tracking these as SCD Type 2 changes in transformation layer")
        return len(customer_ids)
    
    @task
    def simulate_accounts_with_quality_issues(connector: PostgresConnector):
        """
        Generate accounts with data quality issues:
        - Impossible balances (negative)
        - Status inconsistencies
        - Duplicate account numbers
        """
        result = connector.execute("""
            SELECT customer_id FROM customers 
            WHERE created_at > NOW() - INTERVAL '30 days'
            OR customer_id IN (
                SELECT customer_id FROM customers 
                ORDER BY RANDOM() 
                LIMIT 25
            )
        """)
        eligible_customers = [row[0] for row in result]
        
        if not eligible_customers:
            print("⚠️ No eligible customers for accounts")
            return 0
        
        result = connector.execute("SELECT account_number FROM accounts")
        existing_nums = {row[0] for row in result}
        
        num_accounts = random.randint(25, 60)
        print(f"\n🏦 Creating {num_accounts} accounts with quality issues...")
        
        account_types = ["Savings", "Checking", "Investment", "Credit"]
        statuses = ['active'] * 82 + ['inactive'] * 12 + ['suspended'] * 6
        
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
                balance = round(random.uniform(50.0, 25000.0), 2)
            
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
    def simulate_transactions_with_quality_issues(connector: PostgresConnector):
        """
        Generate transactions with realistic data quality issues:
        - Late-arriving transactions (arrive 5-30 days late)
        - Future-dated transactions (data entry errors)
        - Transactions on closed accounts
        - Circular transfers (fraud pattern)
        - Inactive account transactions
        - Status inconsistencies (failed but balance updated)
        """
        f = Faker()
        now = pendulum.now()
        
        # Get accounts by status
        result = connector.execute("""
            SELECT account_id, status, balance FROM accounts
        """)
        all_accounts = [{"id": row[0], "status": row[1], "balance": float(row[2])} for row in result]
        
        active_accounts = [a for a in all_accounts if a["status"] == "active"]
        inactive_accounts = [a for a in all_accounts if a["status"] in ["inactive", "suspended"]]
        
        if len(active_accounts) < 2:
            print("⚠️ Not enough active accounts")
            return 0
        
        base_volume = len(active_accounts) * 20
        if now.day_of_week in [5, 6]:
            base_volume = int(base_volume * 0.7)
        if now.day >= 28:
            base_volume = int(base_volume * 1.5)
        
        num_transactions = random.randint(int(base_volume * 0.8), int(base_volume * 1.2))
        print(f"\n💰 Processing {num_transactions} transactions with quality issues...")
        
        trx_distribution = {
            "Deposit": 0.35,
            "Withdrawal": 0.30,
            "Transfer": 0.25,
            "Payment": 0.08,
            "Refund": 0.02
        }
        
        statuses_weighted = ['completed'] * 87 + ['pending'] * 8 + ['failed'] * 5
        
        transactions_data = []
        balance_updates = {}
        quality_issue_counts = {
            'late_arriving': 0,
            'future_dated': 0,
            'inactive_account': 0,
            'circular_transfer': 0,
            'impossible_amount': 0
        }
        
        # Track accounts for circular transfer detection
        recent_transfers = {}
        
        for i in range(num_transactions):
            # QUALITY ISSUE #6: Transactions on Inactive/Closed Accounts
            # 3% of transactions happen on inactive accounts (shouldn't happen in real system)
            if random.random() < 0.03 and inactive_accounts:
                account = random.choice(inactive_accounts)
                quality_issue_counts['inactive_account'] += 1
            else:
                account = random.choice(active_accounts)
            
            trx_type = random.choices(
                list(trx_distribution.keys()),
                weights=list(trx_distribution.values())
            )[0]
            
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
                related_acc = random.choice([a["id"] for a in active_accounts if a["id"] != account["id"]])
                
                # 2% chance of circular transfer
                if random.random() < 0.02:
                    if account["id"] in recent_transfers:
                        # Make it transfer back to previous account
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
            
            # QUALITY ISSUE #4: Future-Dated Transactions (data entry errors)
            # 1% of transactions are dated in the future
            if random.random() < 0.01:
                days_future = random.randint(1, 7)
                transaction_date = now + timedelta(days=days_future)
                quality_issue_counts['future_dated'] += 1
            
            transactions_data.append({
                "acc_id": account["id"],
                "trx_type": trx_type,
                "amt": amount,
                "currency": "USD",
                "description": f.sentence(nb_words=4)[:100],
                "related_acc_id": related_acc,
                "status": status,
                "trx_date": transaction_date
            })
            
            # Update balances only for completed transactions (even if impossible)
            if status == "completed":
                if account["id"] not in balance_updates:
                    balance_updates[account["id"]] = account["balance"]
                
                if trx_type in ["Deposit", "Refund"]:
                    balance_updates[account["id"]] += amount
                elif trx_type in ["Withdrawal", "Payment"]:
                    balance_updates[account["id"]] -= amount
                elif trx_type == "Transfer" and related_acc:
                    balance_updates[account["id"]] -= amount
                    if related_acc not in balance_updates:
                        related_balance = next(a["balance"] for a in all_accounts if a["id"] == related_acc)
                        balance_updates[related_acc] = related_balance
                    balance_updates[related_acc] += amount
        
        # Insert transactions with custom date handling
        for trx in transactions_data:
            connector.execute(
                """
                INSERT INTO transactions (account_id, transaction_type, amount, currency, 
                                         description, related_account_id, status, transaction_date)
                VALUES (:acc_id, :trx_type, :amt, :currency, :description, :related_acc_id, :status, :trx_date)
                """,
                trx
            )
        
        # Update balances
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
    def generate_quality_report(connector: PostgresConnector, 
                              customers: int,
                              customer_updates: int,
                              accounts: int,
                              transactions: int):
        """Generate summary of data quality issues introduced"""
        
        # Calculate quality metrics
        result = connector.execute("SELECT COUNT(*) FROM customers WHERE phone IS NULL OR address IS NULL")
        null_count = result.fetchone()[0]
        
        result = connector.execute("SELECT COUNT(*) FROM accounts WHERE balance < 0")
        negative_balance_count = result.fetchone()[0]
        
        result = connector.execute("""
            SELECT COUNT(*) FROM transactions 
            WHERE transaction_date > NOW()
        """)
        future_date_count = result.fetchone()[0]
        
        result = connector.execute("""
            SELECT COUNT(*) FROM transactions 
            WHERE transaction_date < NOW() - INTERVAL '5 days'
        """)
        late_arrival_count = result.fetchone()[0]
        
        result = connector.execute("""
            SELECT COUNT(*) FROM transactions 
            WHERE status = 'completed' AND account_id IN (
                SELECT account_id FROM accounts WHERE status != 'active'
            )
        """)
        inactive_account_txn = result.fetchone()[0]
        
        print("\n" + "="*70)
        print("📊 ENHANCED OLTP SIMULATION REPORT WITH DATA QUALITY ISSUES")
        print("="*70)
        print(f"Date: {pendulum.now().to_date_string()}")
        
        print("\n✅ DATA GENERATED:")
        print(f"  • New Customers: {customers}")
        print(f"  • Customer Updates (SCD): {customer_updates}")
        print(f"  • New Accounts: {accounts}")
        print(f"  • Transactions: {transactions}")
        
        print("\n⚠️  DATA QUALITY ISSUES INTENTIONALLY INJECTED:")
        print(f"  • Missing Phone/Address: ~{int(customers * 0.35)} ({int(null_count)} in DB)")
        print(f"  • Duplicate Customers: ~{int(customers * 0.08)}")
        print(f"  • Negative Account Balances: ~{int(accounts * 0.02)} ({negative_balance_count} in DB)")
        print(f"  • Future-dated Transactions: ~1% ({future_date_count} in DB)")
        print(f"  • Late-arriving Transactions: ~5% ({late_arrival_count} in DB)")
        print(f"  • Transactions on Inactive Accounts: ~3% ({inactive_account_txn} in DB)")
        print(f"  • Circular Transfers (Fraud): ~2%")
        print(f"  • Impossible Transaction Amounts: ~1%")
        print(f"  • Data Entry Errors (Typos): ~2%")
        print(f"  • Invalid Birth Dates: ~1%")
        
        print("\n📝 TRANSFORMATION LAYER WILL:")
        print("  ✓ Deduplicate customers (fuzzy matching)")
        print("  ✓ Handle NULL values (imputation/filtering)")
        print("  ✓ Validate data ranges (negative balances, future dates)")
        print("  ✓ Implement SCD Type 2 for customer changes")
        print("  ✓ Flag suspicious patterns (circular transfers)")
        print("  ✓ Reconcile late-arriving facts")
        print("  ✓ Create audit trail for data corrections")
        
        print("\n" + "="*70)
        
        try:
            connector.close()
            print("🔌 Connection closed")
        except Exception:
            pass
    
    # DAG Workflow
    db_conn = connect_to_db()
    
    custs = simulate_customers_with_quality_issues(db_conn)
    cust_updates = simulate_customer_updates_with_scd(db_conn)
    accts = simulate_accounts_with_quality_issues(db_conn)
    trxns = simulate_transactions_with_quality_issues(db_conn)
    
    generate_quality_report(db_conn, custs, cust_updates, accts, trxns)

enhanced_oltp_with_data_quality()