import threading
from scraper import scrape_and_save
from database import import_csv_to_db, create_db_connection
from tqdm import tqdm
import pandas as pd
import concurrent.futures
import os

#LOAD ENV VARIABLES
load_dotenv()

#Inizialize global stop event
stop_scraper= threading.Event()

def main():
    #read input data
    input_file="connect_minsan.csv"
    output_file=os.getenv('SCRAPED_CSV')
    df=pd.read_csv(input_file, dtype=str)
    codes= df['minsan'][:100]

    #create csv header
    with open(input_file, 'w') as f:
        f.write("venditore,prezzo,spedizione,minsan,data\n")
    
    #start scraping with multithreading
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures=[
            executor.submit(scrape_and_save, code, stop_scraper,output_file)
            for code in tqdm(codes, desc="Scraping progress",ncols=100)
        ]
        concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)

    #import CSV data into the database
    conn=create_db_connection()
    import_csv_to_db(output_file,"scraping",conn)
    conn.close()

if __name__ =="__main__":
    try:
        stop_scraper.clear()
        main()
    except KeyboardInterrupt:
        print("Scraper interrupted by user.")
        stop_scraper.set()