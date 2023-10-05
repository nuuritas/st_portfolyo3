import os
import pandas as pd
import warnings; warnings.simplefilter('ignore')
import numpy as np
import json
import requests
import concurrent.futures
from tqdm import tqdm
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timedelta
os.chdir(os.path.dirname(os.path.abspath(__file__)))

tv = TvDatafeed()

def convert_sector_wide(data, sector_name):
    rename_dict = {
        "Sektör Ortalamaları": "Metrics",
        "F/K": "fk",
        "PD/DD": "pd_dd",
        "FD/FAVÖK": "fd_favok"
    }
    
    data = data.rename(columns=rename_dict)

    
    new_columns = {
        "BIST 100": "bist100",
        "Aritmetik Ortalama": "ao",
        "Ağırlıklı Ortalama": "wo",
        "Medyan": "median"
    }

    
    wide_df = pd.DataFrame()
    wide_df['sector_name'] = [sector_name]

    for metric, prefix in new_columns.items():
        for column in ['fk', 'pd_dd', 'fd_favok']:
            col_name = f"{prefix}_{column}"
            if sector_name == 'bankacilik' and column == 'fd_favok':
                wide_df[col_name] = np.nan
            else:
                wide_df[col_name] = data[data['Metrics'] == metric][column].values

    return wide_df

def convert_piyasa_degeri(value):
    value = value.replace('₺', '').strip()
    if 'mr' in value:
        value = float(value.replace('mr', '')) * 1e3  # convert to billion
    elif 'mn' in value:
        value = float(value.replace('mn', ''))  # convert to million
    return value

def get_sector(sector_name):

    headers = {
        'authority': 'fintables.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,tr;q=0.8,tr-TR;q=0.7',
        'cache-control': 'no-cache',
        'cookie': '_gid=GA1.2.50961081.1690710140; _gcl_au=1.1.518997462.1690710149; auth-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoyMTIyNzEwMTk3LCJpYXQiOjE2OTA3MTAxOTcsImp0aSI6IjQ2NGI0YTIxYjY3ZjQ3ZDY4MmEwYjg5NWE3ZjlkMWE4IiwidXNlcl9pZCI6MTEyNzMzfQ.Bh3945i5RjYHblFOyoN_e9oqVmQcOUukFo8GqXp5wtg; _gat_UA-72451211-3=1; _ga=GA1.2.1134893438.1690710140; _ga_22JQCWWZZJ=GS1.1.1690710149.1.1.1690711335.20.0.0',
        'dnt': '1',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    response = requests.get(f'https://fintables.com/sektorler/{sector_name}', headers=headers)

    # The content of the response
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    sektor_ozet = soup.find_all('table', class_="min-w-full")[0]
    sektor_ozet2 = str(sektor_ozet).replace(".","").replace(',', '.')
    sektor_ozet_df = pd.read_html(str(sektor_ozet2))[0]
    sektor_ozet_wide = convert_sector_wide(sektor_ozet_df, sector_name)
    
    my_table = soup.find_all('table', class_="min-w-full")[1]
    my_table2 = str(my_table).replace(".","").replace(',', '.')
    df = pd.read_html(str(my_table2))[0]
    
    df['Piyasa Değeri'] = df['Piyasa Değeri'].apply(convert_piyasa_degeri)
    #df['Piyasa Değeri'] = df['Piyasa Değeri'].astype(int)
    df["sector"] = sector_name

    return sektor_ozet_wide, df

def get_sector_multiple(sector_names):
    ozet_list = []
    sirket_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for sektor_ozet,tum_sirketler in tqdm(executor.map(get_sector, sector_names), total=len(sector_names), desc="Fintables Şirketler"):
            try:
                sirket_list.append(tum_sirketler)
                ozet_list.append(sektor_ozet)
            except Exception as e:
                print("Error: ", e)
    sirket_df = pd.concat(sirket_list, axis=0, ignore_index=True)
    ozet_df = pd.concat(ozet_list, axis=0, ignore_index=True)

    sirket_df['Şirket Kodu'] = sirket_df['Şirket Kodu'].str[:-7]
    # sirket_df['Piyasa Değeri'] = sirket_df['Piyasa Değeri'].astype(float)

    sirket_df.columns = ['sirket_kodu', 'piyasa_degeri', 'fk', 'pd_dd', 'fd_favok', 'sector']
    return ozet_df, sirket_df

def fetch_data(ticker):
    try:
        data = tv.get_hist(symbol=ticker, exchange='BIST', interval=Interval.in_daily, n_bars=200)
        return data
    except Exception as e:
        print("Error: ", e)
        return pd.DataFrame()

sector_names = json.load(open('../data/json/sector_names.json',encoding="utf-8"))
print("Fintables Sektörler ve Şirketler Güncelleniyor")
ozet_df, sirket_df = get_sector_multiple(sector_names)
print("Fintables Sektörler ve Şirketler Güncellendi")
all_tickers = list(sirket_df['sirket_kodu'].unique())
all_tickers.append('XU100')

data = pd.read_parquet("../data/parquet/tvdata23.parquet")

now = datetime.now()
if now.weekday() >= 5:  # 5: Saturday, 6: Sunday
    # If today is Saturday, subtract 1 day to get Friday's data
    # If today is Sunday, subtract 2 days to get Friday's data
    days_to_subtract = now.weekday() - 4
    today = now - timedelta(days=days_to_subtract)
    today_str = (now - timedelta(days=days_to_subtract)).strftime("%d-%m-%Y")
else:
    # For weekdays, if the current time is before 18:00, use yesterday's date
    if now.hour < 18:
        today = now - timedelta(days=1)
        today_str = (now - timedelta(days=1)).strftime("%d-%m-%Y")
    else:
        today = now
        today_str = now.strftime("%d-%m-%Y")

day_to_fetch = (today - data.date.max()).days
print(f"Fetching {day_to_fetch} days of data")
all_tickers = list(sirket_df['sirket_kodu'].unique())
all_tickers.append('XU100')
tickers_exist = set(data.ticker.unique()).intersection(all_tickers)
new_tickers = list(set(all_tickers) - tickers_exist)
print(f"Fetching data for {len(new_tickers)} new tickers")

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    data_new_list = list(tqdm(executor.map(fetch_data, new_tickers), total=len(new_tickers)))

try:
    data_new = pd.concat(data_new_list).reset_index()
    data_new["symbol"] = data_new["symbol"].str[5:]
    data_new["date"] = data_new["datetime"].apply(lambda x: x.normalize())
    data_new.drop(columns=['datetime'], inplace=True)
    data_new.rename(columns={'symbol': 'ticker'}, inplace=True)
except Exception as e:
    print("Error: ", e)
    data_new = pd.DataFrame()

def fetch_data_update(ticker):
    try:
        data = tv.get_hist(symbol=ticker, exchange='BIST', interval=Interval.in_daily, n_bars=day_to_fetch)
        return data
    except Exception as e:
        print(f"Error for ticker {ticker}: {e}")
        return pd.DataFrame()

print(f"Fetching data for {len(tickers_exist)} existing tickers")
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Wrap the executor and the ticker list with tqdm for a progress bar
    data_update_list = list(tqdm(executor.map(fetch_data_update, tickers_exist), total=len(tickers_exist)))

data_update = pd.concat(data_update_list).reset_index()
data_update["symbol"] = data_update["symbol"].str[5:]
data_update["date"] = data_update["datetime"].apply(lambda x: x.normalize())
data_update.drop(columns=['datetime'], inplace=True)
data_update.rename(columns={'symbol': 'ticker'}, inplace=True)

data_all = pd.concat([data,data_new,data_update]).drop_duplicates(subset=["ticker","date"])
data_all.to_parquet("../data/parquet/tvdata23.parquet")
print(f"New Data has {data_all.ticker.nunique()} tickers and {data_all.shape[0]} rows")