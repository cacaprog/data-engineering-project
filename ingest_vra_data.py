import os
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
from datetime import datetime

# Database connection details
DATABASE_URI = "postgresql+psycopg2://username:password@localhost:5432/database_name"

# Base URL for the CSV files
BASE_URL = "https://siros.anac.gov.br/siros/registros/diversos/vra/2024/VRA_2024_{:02d}.csv"

# Directory to temporarily store downloaded CSV files
TEMP_DIR = "temp_csv_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Batch size (number of rows to process at a time)
BATCH_SIZE = 10000

# Function to download a CSV file
def download_csv(month: int) -> str:
    url = BASE_URL.format(month)
    response = requests.get(url)
    if response.status_code == 200:
        file_path = os.path.join(TEMP_DIR, f"vra_2024_{month:02d}.csv")
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded CSV for month {month:02d}")
        return file_path
    else:
        print(f"Failed to download CSV for month {month:02d}")
        return None

# Function to ingest CSV data into PostgreSQL in batches
def ingest_csv_to_postgres(file_path: str, month: int):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(DATABASE_URI)
        
        # Define table name
        table_name = f"vra_2024_{month:02d}"
        
        # Read CSV in chunks using pandas
        for chunk in pd.read_csv(file_path, chunksize=BATCH_SIZE):
            # Ingest each chunk into PostgreSQL
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"Ingested batch of {len(chunk)} rows into table {table_name}")
        
        print(f"Finished ingesting data into table {table_name}")
    except exc.SQLAlchemyError as e:
        print(f"Error ingesting data for month {month:02d}: {e}")
    except Exception as e:
        print(f"Unexpected error for month {month:02d}: {e}")

# Main function to process all files
def main():
    for month in range(1, 13):  # Loop through months 1 to 12
        file_path = download_csv(month)
        if file_path:
            ingest_csv_to_postgres(file_path, month)
            # Clean up downloaded file
            os.remove(file_path)
            print(f"Removed temporary file for month {month:02d}")

if __name__ == "__main__":
    main()