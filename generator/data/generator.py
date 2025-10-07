from faker import Faker
from schemas import SchemaLoader
from database import DatabaseLoader

fucker = Faker()

db_loader = DatabaseLoader("postgresql")
schema_loader = SchemaLoader(engine=db_loader.engine)
schemas = schema_loader.tables

print(schemas)