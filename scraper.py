import requests 
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import os

#load env variables
from dotenv import load_dotenv
load_dotenv()

BASE_URL=os.getenv("BASE_URL")

def scraper(code,stop_scraper):
    if stop_scraper.is_set():
        return []
    url=f"{BASE_URL}{code}.aspx"
    response= requests.get(url)
    soup=BeautifulSoup(response.content,"html.parser")
    prices=soup.find_all(class_='item_basic_price')
    sped_prices=soup.find_all(class_='item_delivery_price')
    sellers=soup.find_all(class_="merchant_name")

    results=[]
    for p,s,v in zip(prices,sped_prices,sellers):
        price=p.text.strip().replace(' €', '')
        shipping = s.text.strip().replace('+ Sped. ', '').replace(' €', '').replace('Sped. gratuita', '0,00')
        seller = v.text.strip()
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        results.append((seller, price, shipping, code, date))
        
        if len(results) == 10:
            break

    return results

def save_to_csv(data, filename):
    df = pd.DataFrame(data, columns=["venditore", "prezzo", "spedizione", "minsan", "data"])
    df.to_csv(filename, mode='a', header=False, index=False)


def scrape_and_save(code, stop_scraper, output_file):
    if stop_scraper.is_set():
        return
    data = scraper(code, stop_scraper)
    save_to_csv(data, output_file)