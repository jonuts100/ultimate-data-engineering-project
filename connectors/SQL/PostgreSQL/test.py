"""
File untuk test postgresql connection

Untuk sementara, gw test pake postgresql running on docker and local postgresql dlu
Karena gw lg di linux, gw cm bs test postgresql sm mysql

TO-DO
1. Create postgresql database local and docker
2. Get db credentials and store in a dictionary for now
3. Create a menu that takes user input
4. Create instance of DBConfig and pass user input into the DBConfig instance
5. Check if PostgresConfig works as intended
6. Create instance of connector and pass PostgresConfig instance as a parameter
7. Connect to db from python
8. Check if PostgresConnector works as intended
9. If both things work, try querying from the database
10. Make a script to query all the tables in the database (full load)
11. Then, make a batch load script to query all the new rows

REQUIREMENTS
1. Make PostgreSQL db, fill it with sample data
2. Create fake data sampling script that runs every 1 hour
"""