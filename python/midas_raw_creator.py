import pandas as pd
from datetime import datetime as dt
import numpy as np

def midas_raw_creator(portfoy_files, investment_files, hesap_files):
    investment_df = pd.concat(investment_files, axis=0, ignore_index=True)
    hesap_df = pd.concat(hesap_files, axis=0, ignore_index=True)
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

    return investment_df, cum_inv_df