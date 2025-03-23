import os
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv  # Import the load_dotenv function

# Load environment variables from .env file
load_dotenv()

# Database connection details
DATABASE_URI = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# URL for the CSV file (January 2024)
CSV_URL = "https://siros.anac.gov.br/siros/registros/diversos/vra/2024/VRA_2024_01.csv"

# Directory to temporarily store downloaded CSV files
TEMP_DIR = "temp_csv_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Batch size (number of rows to process at a time)
BATCH_SIZE = 10000

# Function to download the CSV file
def download_csv() -> str:
    response = requests.get(CSV_URL)
    if response.status_code == 200:
        file_path = os.path.join(TEMP_DIR, "vra_2024_01.csv")
        with open(file_path, "wb") as file:
            file.write(response.content)
        print("Downloaded CSV file successfully.")
        return file_path
    else:
        print("Failed to download CSV file.")
        return None

# Function to ingest CSV data into PostgreSQL in batches
def ingest_csv_to_postgres(file_path: str):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(DATABASE_URI)
        
        # Define table name
        table_name = "vra_2024_01"
        
        # Read CSV in chunks using pandas
        for chunk in pd.read_csv(file_path, chunksize=BATCH_SIZE):
            # Ingest each chunk into PostgreSQL
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"Ingested batch of {len(chunk)} rows into table {table_name}")
        
        print(f"Finished ingesting data into table {table_name}")
    except exc.SQLAlchemyError as e:
        print(f"Error ingesting data: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Main function to process the file
def main():
    file_path = download_csv()
    if file_path:
        ingest_csv_to_postgres(file_path)
        # Clean up downloaded file
        os.remove(file_path)
        print("Removed temporary file.")

if __name__ == "__main__":
    main()