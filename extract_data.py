import os
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, MetaData
from sqlalchemy.types import Text

# Database connection parameters
db_config = {
    'dbname': 'brazil_vra',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# URL and file names
base_url = "https://siros.anac.gov.br/siros/registros/diversos/vra/2024/"
file_names = [f"vra_{str(i).zfill(2)}_2024.csv" for i in range(1, 13)]  # Generates file names from 01 to 12

# Directory to save downloaded files
download_dir = "downloaded_csvs"
os.makedirs(download_dir, exist_ok=True)

# Function to download files
def download_files():
    for file_name in file_names:
        url = base_url + file_name
        response = requests.get(url)
        if response.status_code == 200:
            file_path = os.path.join(download_dir, file_name)
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded {file_name}")
        else:
            print(f"Failed to download {file_name}")

# Function to load CSV into PostgreSQL using SQLAlchemy
def load_csv_to_postgres():
    # Create a SQLAlchemy engine
    engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    metadata = MetaData()

    for file_name in file_names:
        file_path = os.path.join(download_dir, file_name)
        if os.path.exists(file_path):
            # Read CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)

            # Infer table name from file name
            table_name = file_name.replace('.csv', '')

            # Automatically create table schema based on DataFrame columns
            # Use Text as the default type for all columns
            dtype = {col: Text for col in df.columns}

            dtype = {
                'column1': Text,
                'column2': Integer,
                'column3': Float,
            }

            # Load DataFrame into PostgreSQL
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',  # Replace table if it already exists
                index=False,          # Do not include DataFrame index as a column
                dtype=dtype           # Use inferred schema
            )

            print(f"Loaded {file_name} into PostgreSQL as table '{table_name}'")
        else:
            print(f"File {file_name} does not exist")

# Main function
def main():
    download_files()
    load_csv_to_postgres()

if __name__ == "__main__":
    main()