from sqlalchemy import (
    Table, Column, Integer, String, Date, DateTime, Text,
    Numeric, ForeignKey, MetaData, func, Sequence
)

class SchemaLoader:
    def __init__(self, engine=None):
        self.engine = engine
        self.metadata_obj = MetaData()
        self.tables = self._load_schemas()

    def _load_schemas(self):
        # --- CUSTOMERS TABLE ---
        customers = Table(
            "customers",
            self.metadata_obj,
            Column("customer_id", Integer, primary_key=True, autoincrement=True),
            Column("full_name", Text, nullable=False),
            Column("email", Text, nullable=False, unique=True, index=True),
            Column("phone", Text),
            Column("address", Text),
            Column("date_of_birth", Date),
            Column("created_at", DateTime, server_default=func.now(), nullable=False),
            Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now(), nullable=False),
        )

        # --- ACCOUNTS TABLE ---
        accounts = Table(
            "accounts",
            self.metadata_obj,
            Column("account_id", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", Integer, ForeignKey("customers.customer_id"), nullable=False),
            Column("account_number", String(20), unique=True, nullable=False, index=True),
            Column("account_type", String(50), nullable=False),  # e.g. savings, checking
            Column("balance", Numeric(15, 2), nullable=False, default=0.00),
            Column("currency", String(10), nullable=False, default="USD"),
            Column("status", String(20), nullable=False, default="active"),
            Column("opened_at", DateTime, server_default=func.now(), nullable=False),
            Column("closed_at", DateTime, nullable=True),
        )

        # --- TRANSACTIONS TABLE ---
        transactions = Table(
            "transactions",
            self.metadata_obj,
            Column("transaction_id", Integer, primary_key=True, autoincrement=True),
            Column("account_id", Integer, ForeignKey("accounts.account_id"), nullable=False),
            Column("transaction_type", String(20), nullable=False),  # deposit, withdrawal, transfer
            Column("amount", Numeric(15, 2), nullable=False),
            Column("currency", String(10), nullable=False, default="USD"),
            Column("transaction_date", DateTime, server_default=func.now(), nullable=False),
            Column("description", Text, nullable=True),
            Column("related_account_id", Integer, ForeignKey("accounts.account_id"), nullable=True),
            Column("status", String(20), nullable=False, default="completed"),
            Column("created_at", DateTime, server_default=func.now(), nullable=False),
        )
        self.metadata_obj.create_all(self.engine)
        # Store for external access
        return {
            "customers": customers,
            "accounts": accounts,
            "transactions": transactions
        }
