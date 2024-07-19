from pymongo import MongoClient
import pandas as pd
import duckdb
import schedule
import time
import os

# MongoDB connection details
server_address = '65.2.90.153'
port = 22020
username = 'admin'
password = 'admin'
database_name = 'interface'
collection_name = 'transaction_summary'

# MongoDB URI without replica set
mongo_uri = f'mongodb://{username}:{password}@{server_address}:{port}/{database_name}?authSource=admin'

# DuckDB database file name
duckdb_file = 'my_different_database1.duckdb'

# CSV file name
csv_file = 'Interface 1.csv'

def fetch_and_push_data():
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Fetch data from MongoDB
        data = list(collection.find({}))

        # Convert data to DataFrame
        df = pd.DataFrame(data)

        # Optional: Drop '_id' column if it exists
        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        # Convert all columns to strings to avoid type casting issues
        df = df.astype(str)

        try:
            # Connect to DuckDB
            con = duckdb.connect(duckdb_file)

            # Drop the existing table and create a new one
            con.execute("DROP TABLE IF EXISTS Interface")
            con.execute("CREATE TABLE Interface AS SELECT * FROM df")

            print("Data saved to DuckDB successfully!")

            # Save data to CSV file
            df.to_csv(csv_file, index=False)
            print(f"Data saved to {csv_file} successfully!")

        except Exception as e:
            print(f"An error occurred while saving to DuckDB: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Schedule the data fetching and pushing every 100 seconds
schedule.every(1000).seconds.do(fetch_and_push_data)

# Infinite loop to run the scheduler
try:
    while True:
        schedule.run_pending()
        time.sleep(1000)
except KeyboardInterrupt:
    print("Process interrupted by user.")
