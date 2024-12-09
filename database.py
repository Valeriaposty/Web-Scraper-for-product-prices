import pymysql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_db_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT")),
    )

def insert_data(conn, row, table_name):
    venditore, prezzo, spedizione, minsan, data = row
    query = f"INSERT INTO {table_name} (venditore, prezzo, spedizione, minsan, data) VALUES (%s, %s, %s, %s, %s)"
    values = (venditore, prezzo, spedizione, minsan, data)
    
    with conn.cursor() as cur:
        cur.execute(query, values)
        conn.commit()

def import_csv_to_db(csv_file_path, table_name, conn):
    df = pd.read_csv(csv_file_path)
    for _, row in df.iterrows():
        insert_data(conn, row, table_name)
        print(f"Inserted row: {row.to_dict()}")

''''CREATE TABLE scraping (
    venditore VARCHAR(255),
    prezzo FLOAT,
    spedizione FLOAT,
    minsan VARCHAR(255),
    data DATETIME
);
''''