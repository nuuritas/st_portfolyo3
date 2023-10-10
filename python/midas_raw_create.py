from io import StringIO
import re
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime as dt
import numpy as nps
import os
from midas_exporter import midas_exporter
from excel_transformer import excel_transformer

os.chdir(os.path.dirname(__file__))

dir_path = "../data/midas_pdf"

files = os.listdir(dir_path)
files = [f for f in files if 'midas_' in f and f.endswith('.pdf')]

# Lists to store dataframes
portfoy_dfs = []
investment_dfs = []
hesap_dfs = []

for file in sorted(files):
    file_path = os.path.join(dir_path, file)
    
    portfoy_df_temp, investment_df_temp, hesap_df_temp = midas_exporter(file_path)
    
    portfoy_dfs.append(portfoy_df_temp)
    investment_dfs.append(investment_df_temp)
    hesap_dfs.append(hesap_df_temp)

investment_df = pd.concat(investment_dfs, axis=0, ignore_index=True)

# portfoy_df_7, investment_df_7, hesap_df_7 = midas_exporter("../data/midas_pdf/midas_7.pdf")
# portfoy_df_8, investment_df_8, hesap_df_8 = midas_exporter("../data/midas_pdf/midas_8.pdf")
# portfoy_df_9, investment_df_9, hesap_df_9 = midas_exporter("../data/midas_pdf/midas_9.pdf")
# investment_df = pd.concat([investment_df_7, investment_df_8, investment_df_9], axis=0, ignore_index=True)

# hesap_df = pd.concat([hesap_df_7, hesap_df_8,hesap_df_9], axis=0, ignore_index=True)
hesap_df = pd.concat(hesap_dfs, axis=0, ignore_index=True)
hesap_df["date"] = hesap_df["İşlem Tarihi"].apply(lambda x: dt.strptime(x, "%d/%m/%y %H:%M:%S"))
hesap_df["date"] = hesap_df["date"].apply(lambda x: x.normalize())
hesap_df["adj_amount"] = np.where(hesap_df["İşlem Tipi"] == "Para Çekme", -hesap_df["Tutar (YP)"], hesap_df["Tutar (YP)"])
hesap_df["adj_amount"] = hesap_df["adj_amount"].astype(int)

sembol_list = investment_df.Sembol.unique().tolist()
sembol_list.remove("ALTIN.S1")
min_date = investment_df["Tarih"].min()
today = dt.today()

investment_df.rename(columns={
    'Tarih': 'date',
    "İşlem Türü": "order_type",
    'Sembol': 'ticker',
    "İşlem Durumu": "status",
    "Para Birimi": "currency",
    "Emir Tutarı": "order_amount",
    'İşlem Tipi': 'buy_sell',
    'Emir Adedi': 'quantity',
    "Gerçekleşen Adet": "realized_q",
    "İşlem Ücreti": "trans_fee",
    "İşlem Tutarı": "trans_amount",
    'Ortalama İşlem Fiyatı': 'price'
}, inplace=True)

if os.path.exists("../data/excel/data.xlsx"):
    excel_data = excel_transformer("../data/excel/data.xlsx")
    investment_df = pd.concat([investment_df, excel_data], axis=0, ignore_index=True)   

investment_df['date'] = pd.to_datetime(investment_df['date'])

investment_df['date'] = investment_df['date'].apply(lambda x: x + pd.tseries.offsets.BusinessDay(1) if x.dayofweek > 5 else x)
investment_df['date'] = investment_df['date'].apply(lambda x: x.normalize())
investment_df['adj_q'] = investment_df.apply(lambda x: -x['quantity'] if x['buy_sell'] == 'Satış' else x['quantity'], axis=1)
daily_range = pd.date_range(start=min_date, end=today, freq='D').normalize() + pd.Timedelta(seconds=1)


cumulative_amount = []
for day in daily_range:
    cumulative_amount.append(hesap_df[hesap_df["date"] <= day]["adj_amount"].sum())
    
cum_inv_df = pd.DataFrame({"date": daily_range, "cum_inv": cumulative_amount})
cum_inv_df["date"] = cum_inv_df["date"].apply(lambda x: x.normalize())



investment_df.to_parquet("../data/midas_raw/midas_df.parquet")
cum_inv_df.to_parquet("../data/midas_raw/midas_cum_inv_df.parquet")