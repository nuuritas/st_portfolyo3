import os
import pandas as pd
import warnings

warnings.simplefilter("ignore")
import numpy as np
import json
import requests
import concurrent.futures
import time


def convert_sector_wide(data, sector_name):
    rename_dict = {
        "Sektör Ortalamaları": "Metrics",
        "F/K": "fk",
        "PD/DD": "pd_dd",
        "FD/FAVÖK": "fd_favok",
    }

    data = data.rename(columns=rename_dict)

    new_columns = {
        "BIST 100": "bist100",
        "Aritmetik Ortalama": "ao",
        "Ağırlıklı Ortalama": "wo",
        "Medyan": "median",
    }

    wide_df = pd.DataFrame()
    wide_df["sector_name"] = [sector_name]

    for metric, prefix in new_columns.items():
        for column in ["fk", "pd_dd", "fd_favok"]:
            col_name = f"{prefix}_{column}"
            if sector_name == "bankacilik" and column == "fd_favok":
                wide_df[col_name] = np.nan
            else:
                wide_df[col_name] = data[data["Metrics"] == metric][column].values

    return wide_df


def convert_piyasa_degeri(value):
    value = value.replace("₺", "").strip()
    if "mr" in value:
        value = float(value.replace("mr", "")) * 1e3  # convert to billion
    elif "mn" in value:
        value = float(value.replace("mn", ""))  # convert to million
    return value


def get_sector(sector_name):
    headers = {
        "authority": "fintables.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,tr;q=0.8,tr-TR;q=0.7",
        "cache-control": "no-cache",
        "cookie": "_gid=GA1.2.50961081.1690710140; _gcl_au=1.1.518997462.1690710149; auth-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoyMTIyNzEwMTk3LCJpYXQiOjE2OTA3MTAxOTcsImp0aSI6IjQ2NGI0YTIxYjY3ZjQ3ZDY4MmEwYjg5NWE3ZjlkMWE4IiwidXNlcl9pZCI6MTEyNzMzfQ.Bh3945i5RjYHblFOyoN_e9oqVmQcOUukFo8GqXp5wtg; _gat_UA-72451211-3=1; _ga=GA1.2.1134893438.1690710140; _ga_22JQCWWZZJ=GS1.1.1690710149.1.1.1690711335.20.0.0",
        "dnt": "1",
        "pragma": "no-cache",
        "sec-ch-ua": '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }

    response = requests.get(
        f"https://fintables.com/sektorler/{sector_name}", headers=headers
    )

    # The content of the response
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(response.content, "html.parser")

    sektor_ozet = soup.find_all("table", class_="min-w-full")[0]
    sektor_ozet2 = str(sektor_ozet).replace(".", "").replace(",", ".")
    sektor_ozet_df = pd.read_html(str(sektor_ozet2))[0]
    sektor_ozet_wide = convert_sector_wide(sektor_ozet_df, sector_name)

    my_table = soup.find_all("table", class_="min-w-full")[1]
    my_table2 = str(my_table).replace(".", "").replace(",", ".")
    df = pd.read_html(str(my_table2))[0]

    df["Piyasa Değeri"] = df["Piyasa Değeri"].apply(convert_piyasa_degeri)
    # df['Piyasa Değeri'] = df['Piyasa Değeri'].astype(int)
    df["sector"] = sector_name

    return sektor_ozet_wide, df


def get_sector_multiple(sector_names):
    ozet_list = []
    sirket_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for sektor_ozet, tum_sirketler in executor.map(get_sector, sector_names):
            try:
                sirket_list.append(tum_sirketler)
                ozet_list.append(sektor_ozet)
            except Exception as e:
                print("Error: ", e)
    sirket_df = pd.concat(sirket_list, axis=0, ignore_index=True)
    ozet_df = pd.concat(ozet_list, axis=0, ignore_index=True)

    sirket_df["Şirket Kodu"] = sirket_df["Şirket Kodu"].str[:-7]
    # sirket_df['Piyasa Değeri'] = sirket_df['Piyasa Değeri'].astype(float)

    sirket_df.columns = [
        "sirket_kodu",
        "piyasa_degeri",
        "fk",
        "pd_dd",
        "fd_favok",
        "sector",
    ]
    return ozet_df, sirket_df


# sector_names = json.load(open('../data/json/sector_names.json',encoding="utf-8"))
sector_names = [
    "ambalaj",
    "ana-metal",
    "araci-kurum",
    "bankacilik",
    "bilisim-ve-yazilim",
    "cam-seramik-porselen",
    "dayanikli-tuketim-urunleri",
    "destek-ve-hizmet",
    "emeklilik",
    "enerji-teknolojileri",
    "enerji-uretim-ve-dagitim",
    "faktoring",
    "finansal-kiralama",
    "gayrimenkul",
    "gida-perakendeciligi",
    "gida-ve-icecek",
    "girisim-sermayesi-yatirim-ortakligi",
    "giyim-tekstil-ve-deri-urunleri-perakendeciligi",
    "haberlesme",
    "holding",
    "ilac-ve-saglik",
    "imalat",
    "insaat",
    "kagit-ve-kagit-urunleri",
    "kimya-ve-plastik",
    "madencilik-ve-tas-ocakciligi",
    "menkul-kiymet-yatirim-ortakligi",
    "metal-esya-ve-makine",
    "mobilya-ve-dekorasyon",
    "otomotiv",
    "otomotiv-yan-sanayi",
    "savunma",
    "servis-tasimaciligi-ve-arac-kiralama",
    "sigorta",
    "spor",
    "tarim-hayvancilik-balikcilik",
    "tasarruf-finansman",
    "tas-toprak-cimento",
    "teknolojik-urun-ticareti",
    "tekstil-giyim-ve-deri",
    "toptan-ve-perakende-ticaret",
    "turizm",
    "ulastirma",
    "varlik-yonetimi",
]
print("Fintables Sektörler ve Şirketler Güncelleniyor")
ozet_df, sirket_df = get_sector_multiple(sector_names)
print("Fintables Sektörler ve Şirketler Güncellendi")

all_tickers = list(sirket_df["sirket_kodu"].unique())
all_tickers.append("XU100")

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47",
    "authority": "www.foreks.com",
    "method": "GET",
    "path": "/api/historical/intraday?code=OFSYM.E.BIST&period=1440&last=100",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "tr,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "dnt": "1",
    "sec-ch-ua": '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

cookies = {
    "i18n_redirected": "tr",
    "pId": "vneta0e861cc-3c89-46a1-b21f-e2a720aecdb6",
    "userID": "c77e2b32-d867-4933-a07c-0b53060cb496",
    "_clck": "1l4vxdg|2|ffb|0|1363",
    "_tt_enable_cookie": "1",
    "_ttp": "TKVHsdwxEB9IQRbZxQ4pxgyyOhr",
    "_gcl_au": "1.1.361451387.1695674357",
    "__hstc": "81955593.f363a4ecc33435f6b82cd4e5799267d2.1695674357108.1695674357108.1695674357108.1",
    "hubspotutk": "f363a4ecc33435f6b82cd4e5799267d2",
    "_ga_TT9Z16KG4K": "GS1.1.1695674355.1.1.1695676366.60.0.0",
    "_gid": "GA1.2.608856078.1696528526",
    "_gat_gtag_UA_82686003_1": "1",
    "_ga": "GA1.1.2132465251.1695674347",
    "CloudFront-Key-Pair-Id": "APKAIVVJE7R23ILHVNCQ",
    "watchID": "64ff5efb-82a6-46ec-a3af-3a7bb788375a",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImZvcmVrcy5jb20iLCJwYXNzd29yZCI6InY0YSFLJTJSIiwiaWF0IjoxNjk2NTI4NTMxfQ.cqrKQwWqLhRgbgiIBi2byJ2PzGhi29g1OyeUS2eVxDo",
    "_ga_4Y6C81V13E": "GS1.1.1696528525.2.1.1696528531.54.0.0",
    "_ga_3HPQ6LZVLP": "GS1.1.1696528525.2.1.1696528531.54.0.0",
    "CloudFront-Signature": "fuQXXzqf5sNtiusAcYqifeHzaCN~bebSRyjP7RrQctAg9UgBfRPH~UxjW92V4GVTd1wi4v3ah0ajEbrRuTjUVHUq60wLigcTJu-veyXhWPKPyEzVhecEhLZmSFTlp52nyyw4dVmV2Qa8Hjneb~wAjc0dpXE11BO7bS3W5p5a4-~Ah7hR5O86mM5kuv7qqiGm3IUXSLwZk3b6NqnKaZMY9-fTBhqNDszxmMFoCiGA8cAOY3u5xvTIriBiXpQvO63TCj0DNeltTBE49Gz~okJCtQUenEOd32rSpOQulg3nJiqczhM5njy3p7SICJPov0yd-f~~uFKP5mctV90f~U5KGg__",
    "CloudFront-Policy": "eyJTdGF0ZW1lbnQiOiBbeyJSZXNvdXJjZSI6Imh0dHBzOi8vbmV3cy1jb250ZW50LmZvcmVrcy5jb20vKiIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsiQVdTOkVwb2NoVGltZSI6MTY5NjUzMjE0N319fV19",
}


def fetch_data_daily(ticker):
    try:
        if ticker == "XU100":
            url = f"https://www.foreks.com/api/historical/intraday?code={ticker}.I.BIST&period=1440&last=1440"
        else:
            url = f"https://www.foreks.com/api/historical/intraday?code={ticker}.E.BIST&period=1440&last=1440"
        response = requests.get(url, headers=headers, cookies=cookies)

        temp = pd.DataFrame(response.json())
        temp["ticker"] = ticker

        return temp
    except Exception as e:
        print(f"Error for ticker {ticker}: {e}")
        return None


def fetch_data_hourly(ticker):
    try:
        if ticker == "XU100":
            url = f"https://www.foreks.com/api/historical/intraday?code={ticker}.I.BIST&period=60&last=1440"
        else:
            url = f"https://www.foreks.com/api/historical/intraday?code={ticker}.E.BIST&period=60&last=1440"
        response = requests.get(url, headers=headers, cookies=cookies)

        temp = pd.DataFrame(response.json())
        temp["ticker"] = ticker

        return temp
    except Exception as e:
        print(f"Error for ticker {ticker}: {e}")
        return None


start = time.time()
all_data_daily = pd.DataFrame()
MAX_WORKERS = 10

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(fetch_data_daily, ticker) for ticker in all_tickers]
    for future in concurrent.futures.as_completed(futures):
        data = future.result()
        if data is not None:
            all_data_daily = pd.concat(
                [all_data_daily, data], axis=0, ignore_index=True
            )
all_data_daily["date"] = pd.to_datetime(all_data_daily["d"], unit="ms") + pd.Timedelta(
    hours=3
)
print(
    f"Time taken for daily: {time.time() - start:.2f} seconds. Number of ticker: {len(all_tickers)}, fetched tickers: {len(all_data_daily.ticker.unique())}"
)

start = time.time()
all_data_hourly = pd.DataFrame()
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(fetch_data_hourly, ticker) for ticker in all_tickers]
    for future in concurrent.futures.as_completed(futures):
        data = future.result()
        if data is not None:
            all_data_hourly = pd.concat(
                [all_data_hourly, data], axis=0, ignore_index=True
            )

all_data_hourly["date"] = pd.to_datetime(
    all_data_hourly["d"], unit="ms"
) + pd.Timedelta(hours=3)
print(
    f"Time taken for hourly {time.time() - start:.2f} seconds. Number of ticker: {len(all_tickers)}, fetched tickers: {len(all_data_hourly.ticker.unique())}"
)

all_data_hourly.drop(columns=["d", "v", "a", "v"], inplace=True)
all_data_daily.drop(columns=["d", "v", "a", "w", "v"], inplace=True)


all_data_daily.columns = ["open", "high", "low", "close", "ticker", "date"]
all_data_hourly.columns = ["open", "high", "low", "close", "ticker", "date"]

all_data_daily["open"] = all_data_daily["close"].shift(1)
mask = all_data_hourly['date'].dt.hour == 9
all_data_hourly.loc[mask, 'open'] = all_data_hourly['close'].shift(1)[mask]

all_data_daily.to_parquet("data/parquet/data_daily.parquet")
all_data_hourly.to_parquet("data/parquet/data_hourly.parquet")
