import re
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime as dt
import os 
script_directory = os.path.dirname(__file__)
os.chdir(script_directory)

def p_no_sell(grouped):
    grouped.insert(2,"d_q_s", 0)
    grouped.insert(3,"d_q_c",grouped["d_q_b"] - grouped["d_q_s"])
    grouped.insert(5,"d_a_s",0)
    grouped["d_p_s"] = 0

    grouped = grouped.fillna(0)
    grouped["h_q"] = grouped["d_q_c"].cumsum()
    

    grouped["a_a_b"] = grouped["d_a_b"].cumsum() 
    grouped["a_a_s"] = grouped["d_a_s"].cumsum() 

    grouped["a_p_b"] = grouped["a_a_b"] / grouped["h_q"]

    grouped["a_p_b"] = grouped["a_p_b"].apply(lambda x: round(x,2))
    grouped["d_r_p"] = 0
    grouped["a_r_p"] = 0
    grouped.insert(9,"h_a",grouped["a_p_b"] * grouped["h_q"])
    grouped["h_a"] = grouped["h_a"].apply(lambda x: round(x,2))
    return grouped

def p_buy_and_sell(grouped):
    grouped = grouped.fillna(0)
    grouped.insert(3,"d_q_c",grouped["d_q_b"] - grouped["d_q_s"])
    grouped["h_q"] = grouped["d_q_c"].cumsum()
    

    grouped["a_a_b"] = grouped["d_a_b"].cumsum() 
    grouped["a_a_s"] = grouped["d_a_s"].cumsum() 

    grouped.loc[0,"a_p_b"] = grouped.loc[0,"a_a_b"] / grouped.loc[0,"h_q"]

    for i, val in grouped.iterrows():
        #Eğer tüm hisseler o gün satıldıysa, bir sonraki gündeki ortalama fiyat sadece yeni alınan hisselerin ortalaması olur.
        #Eğer tüm hisseler o gün satıldıysa, ve o gün alım olmadıysa, ortalama önceki güne eşit olur.
        if val["h_q"] == 0:
            if val["d_q_b"] == 0:
                grouped.loc[i,"a_p_b"] = grouped.loc[i-1,"a_p_b"]
            else:
                pass
        else:
            #Eğer alış olmadıysa, eldeki maliyet değişmez.
            if val["d_q_b"] == 0:
                grouped.loc[i,"a_p_b"] = grouped.loc[i-1,"a_p_b"]
            else:
                grouped.loc[i,"a_p_b"] = (grouped.loc[:i,"d_a_b"].sum() - grouped.loc[:i,"d_a_s"].sum()) / grouped.loc[i,"h_q"]
            # grouped.loc[i,"a_p_b"] = val["a_a_b"] / val["h_q"]
        if 0 in grouped.loc[:i,"h_q"].values:
            
            last_zero_index = grouped.loc[:i,"h_q"].tolist().index(0)
            if val["d_q_b"] != 0:
                grouped.loc[i,"a_p_b"] = sum_product = grouped.loc[last_zero_index:i, 
                                        "d_p_b"].mul(grouped.loc[last_zero_index:i, "d_q_b"]).sum() / grouped.loc[last_zero_index:i,"d_q_b"].sum()
        if val["d_q_s"] > 0:
            last_average_buy = grouped.loc[:i].query("d_p_b != 0")["d_p_b"].iloc[-1]
            grouped.loc[i,"d_r_p"] = (val["d_p_s"] - last_average_buy) * val["d_q_s"]
        else:
            grouped.loc[i,"d_r_p"] = 0
        grouped.loc[i,"a_r_p"] = grouped.loc[:i,"d_r_p"].sum()
    
    grouped["a_p_b"] = grouped["a_p_b"].apply(lambda x: round(x,2))
        
    grouped.insert(9,"h_a",grouped["a_p_b"] * grouped["h_q"])
    grouped["h_a"] = grouped["h_a"].apply(lambda x: round(x,2))
    return grouped

def port_func1(ticker,df):
    data = df.query("ticker == @ticker")
    data["date"] = data["date"].apply(lambda x: x.normalize())

    df1 = data.groupby(["date", "buy_sell"]).agg({
        "quantity": "sum",
        "trans_amount": "sum",
        "price": lambda x: (x * data.loc[x.index, "quantity"]).sum() / df.loc[x.index, "quantity"].sum()
    }).unstack()

    df1.columns = ["_".join(col).strip() for col in df1.columns.values]
    df1 = df1.rename(columns={
        "quantity_Alış": "d_q_b",
        "quantity_Satış": "d_q_s",
        "trans_amount_Alış": "d_a_b",
        "trans_amount_Satış": "d_a_s",
        "price_Alış": "d_p_b",
        "price_Satış": "d_p_s"
    }).reset_index()

    ####Situation stock never sold:
    if "d_q_s" not in df1.columns:
        df2 = p_no_sell(df1)
    else:
        df2 = p_buy_and_sell(df1)
    return ticker, df2

    ticker, df3 = port_func1(ticker,df)

def port_func2(ticker,df3):
        min_date = df3["date"].min()
        if df3.loc[len(df3)-1,"h_q"] != 0:
            max_date = dt.today()
        else:
            max_date = df3["date"].max()
        df4 = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date, freq='B').normalize()})
        df4 = df4.merge(df3, on='date', how='left')
        df4 = df4.fillna({
                'd_q_b': 0,
                'd_q_s': 0,
                'd_a_b': 0,
                'd_a_s': 0,
                'd_p_b': 0,
                'd_p_s': 0,
                'd_q_c': 0,
                "d_r_p": 0,
            })
        
        df4 = df4.merge(price_data.query("ticker == @ticker")[["date","open","close"]], on=["date"],how="left")
        f_fill_col = ["h_q", "a_a_b","a_a_s", "a_p_b", "a_r_p","close","open"] 
        #min, max, vol_ö
        df4[f_fill_col] = df4[f_fill_col].ffill()
        
        df4 = df4.query("d_q_b + d_q_s + h_q > 0")
        # df4 = df4.fillna(0)
        df4["h_a"] = df4["h_q"] * df4["a_p_b"]
        df4['t_v'] = df4['h_q'] * df4['close']
        
        df4['a_ur_p'] = df4['t_v'] - df4['h_a']
        df4['a_ur_p'] = np.where(df4['h_q'] == 0, 0, df4['a_ur_p'])
        df4["d_ur_p"] = (df4["close"] - df4["open"]) * df4["h_q"]
        df4["d_p"] = df4["d_ur_p"] + df4["d_r_p"]
        df4["a_p"] = df4["a_ur_p"] + df4["a_r_p"]
        df4["d_%"] = (round(df4["close"] / df4["open"],3) - 1) * 100
        df4["a_%"] = (round(df4["close"] / df4["a_p_b"],3) - 1) * 100
        df4.reset_index(drop=True, inplace=True)
        df4["d_p_b"] = df4["d_p_b"].apply(lambda x: round(x,2))
        df4["open"] = df4["open"].apply(lambda x: round(x,2))
        df4.insert(1, 'ticker', ticker)
        return df4    
    
def portfoy(ticker):
    ticker, df3 = port_func1(ticker,df)
    df4 = port_func2(ticker,df3)
    return df4

def create_gunluk_ozet(port_all):
    selected_col = ["date","ticker","h_q","a_p_b",'d_q_c',"open","close","d_%",'a_%', 'd_a_b',"d_a_s", 't_v',"d_r_p", 'a_r_p',"d_ur_p", 'a_ur_p']
    gunluk_ozet_raw = port_all[selected_col]

    # Group by business week
    gunluk_ozet = gunluk_ozet_raw.groupby(pd.Grouper(key="date",freq='D')).agg({
        "d_a_b": 'sum',
        "d_a_s":'sum',
        "t_v": 'sum',
        "d_r_p": 'sum',
        "d_ur_p": 'sum',
        "a_r_p": 'sum',
        "a_ur_p": 'sum',
    }).reset_index()
    gunluk_ozet = gunluk_ozet[gunluk_ozet["date"].isin(price_data["date"].unique())]
    gunluk_ozet["d_a_c"] = - gunluk_ozet["d_a_b"] + gunluk_ozet["d_a_s"]
    gunluk_ozet["d_a_c"] = gunluk_ozet["d_a_c"].round(2)
    gunluk_ozet["t_v"] = gunluk_ozet["t_v"].round(2)
    gunluk_ozet.insert(3,"t_v_y",gunluk_ozet["t_v"].shift(1))
    gunluk_ozet.loc[0,"t_v_y"] = gunluk_ozet.loc[0,"d_a_b"]

    gunluk_ozet["d_r_p"] = gunluk_ozet["d_r_p"].round(2)
    gunluk_ozet["d_ur_p"] = gunluk_ozet["d_ur_p"].round(2)

    gunluk_ozet = gunluk_ozet.merge(cum_inv_df, on="date", how="left")
    gunluk_ozet.rename(columns={"cum_inv": "a_inv"}, inplace=True)
    gunluk_ozet.insert(9,"d_inv",gunluk_ozet["a_inv"].diff())
    gunluk_ozet.loc[0,"d_inv"] = gunluk_ozet.loc[0,"a_inv"]
    gunluk_ozet["d_inv"] = gunluk_ozet["d_inv"].astype(int)

    gunluk_ozet.loc[1:,"t_v_y"] = gunluk_ozet.loc[1:,"t_v_y"] + gunluk_ozet.loc[1:,"d_inv"]
    gunluk_ozet.insert(4,"d_%",(round(gunluk_ozet["t_v"] / gunluk_ozet["t_v_y"],4) - 1) * 100)

    gunluk_ozet["d_b"] = gunluk_ozet["d_inv"] + (gunluk_ozet["d_a_c"])
    gunluk_ozet["a_b"] = gunluk_ozet["d_b"].cumsum()

    gunluk_ozet["a_r_p"] = gunluk_ozet["a_r_p"].round(2)
    gunluk_ozet["a_ur_p"] = gunluk_ozet["a_ur_p"].round(2)
    gunluk_ozet["d_p"] = gunluk_ozet["d_r_p"] + gunluk_ozet["d_ur_p"]
    gunluk_ozet["d_p_y"] = round(gunluk_ozet["d_p"] / gunluk_ozet["t_v"],4) * 100

    # gunluk_ozet.to_parquet("gunluk_ozet.parquet")
    return gunluk_ozet

def create_hisse_gunluk(port_all):
    selected_col = ["date","ticker","h_q","a_p_b",'d_q_c',"open","close","d_%",'a_%', 'a_a_b', 't_v',"d_r_p", 'a_r_p',"d_ur_p", 'a_ur_p',"d_p","a_p"]
    hisse_gunluk = port_all[selected_col]
    # hisse_gunluk.to_parquet("hisse_gunluk.parquet")
    return hisse_gunluk

def create_haftalık_ozet(port_all):
    selected_col = ["date","ticker","h_q","a_p_b",'d_q_c',"open","close","d_%",'a_%', 'd_a_b',"d_a_s", 't_v',"d_r_p", 'a_r_p',"d_ur_p", 'a_ur_p',"d_p","a_p"]
    haftalık_data = port_all[selected_col]
    def business_week(date):
        # If the date is a Monday, return the date itself.
        if date.weekday() == 0:  
            return date
        # Otherwise, return the date of the nearest past Monday.
        else:
            return date - pd.Timedelta(days=date.weekday())

    # Group by business week
    haftalık_ozet = haftalık_data.groupby([haftalık_data['date'].apply(business_week)]).agg({
        "d_a_b": 'sum',
        "d_a_s":'sum',
        "t_v": 'sum',
        "d_r_p": 'sum',
        "d_ur_p": 'sum',
        "a_r_p": 'sum',
        "a_ur_p": 'sum',
    }).reset_index()

    haftalık_ozet = haftalık_ozet[haftalık_ozet["date"].isin(price_data["date"].unique())]
    haftalık_ozet["d_a_c"] = - haftalık_ozet["d_a_b"] + haftalık_ozet["d_a_s"]
    haftalık_ozet["d_a_c"] = haftalık_ozet["d_a_c"].round(2)
    haftalık_ozet["t_v"] = haftalık_ozet["t_v"].round(2)
    haftalık_ozet.insert(3,"t_v_y",haftalık_ozet["t_v"].shift(1))
    haftalık_ozet.loc[0,"t_v_y"] = haftalık_ozet.loc[0,"d_a_b"]

    haftalık_ozet["d_r_p"] = haftalık_ozet["d_r_p"].round(2)
    haftalık_ozet["d_ur_p"] = haftalık_ozet["d_ur_p"].round(2)

    haftalık_ozet = haftalık_ozet.merge(cum_inv_df, on="date", how="left")
    haftalık_ozet.rename(columns={"cum_inv": "a_inv"}, inplace=True)
    haftalık_ozet.insert(9,"d_inv",haftalık_ozet["a_inv"].diff())
    haftalık_ozet.loc[0,"d_inv"] = haftalık_ozet.loc[0,"a_inv"]
    haftalık_ozet["d_inv"] = haftalık_ozet["d_inv"].astype(int)

    haftalık_ozet.loc[1:,"t_v_y"] = haftalık_ozet.loc[1:,"t_v_y"] + haftalık_ozet.loc[1:,"d_inv"]
    haftalık_ozet.insert(4,"d_%",(round(haftalık_ozet["t_v"] / haftalık_ozet["t_v_y"],4) - 1) * 100)

    haftalık_ozet["d_b"] = haftalık_ozet["d_inv"] + (haftalık_ozet["d_a_c"])
    haftalık_ozet["a_b"] = haftalık_ozet["d_b"].cumsum()

    haftalık_ozet["a_r_p"] = haftalık_ozet["a_r_p"].round(2)
    haftalık_ozet["a_ur_p"] = haftalık_ozet["a_ur_p"].round(2)
    haftalık_ozet["d_p"] = haftalık_ozet["d_r_p"] + haftalık_ozet["d_ur_p"]
    haftalık_ozet["d_p_y"] = round(haftalık_ozet["d_p"] / haftalık_ozet["t_v"],4) * 100

    # haftalık_ozet.to_parquet("haftalık_ozet.parquet")
    return haftalık_ozet

df = pd.read_parquet("../data/midas_raw/midas_df.parquet")
cum_inv_df = pd.read_parquet("../data/midas_raw/midas_cum_inv_df.parquet")
price_data = pd.read_parquet("../data/parquet/data_daily.parquet")

port_all = pd.DataFrame()
for ticker in df.ticker.unique():
    try:
        port_temp = portfoy(ticker)
        port_all = pd.concat([port_all, port_temp], axis=0, ignore_index=True)
    except Exception as e:
        print(ticker, e)

port_all = port_all.query("ticker != 'ALTIN.S1'")
port_all.reset_index(drop=True, inplace=True)

# Define the conditions
condition_ofsym = ((port_all['ticker'] == 'OFSYM') & (port_all['date'] >= '2023-08-16')) | (port_all['ticker'] != 'OFSYM')
condition_adgyo = ((port_all['ticker'] == 'ADGYO') & (port_all['date'] >= '2023-09-21')) | (port_all['ticker'] != 'ADGYO')

# Combine the conditions and filter the DataFrame
port_all = port_all[condition_ofsym & condition_adgyo]
port_all.to_parquet("../data/parquet/port_all.parquet")

gunluk_ozet = create_gunluk_ozet(port_all)
hisse_gunluk = create_hisse_gunluk(port_all)
haftalık_ozet = create_haftalık_ozet(port_all)

gunluk_ozet.to_parquet("../data/parquet/gunluk_ozet.parquet")
hisse_gunluk.to_parquet("../data/parquet/hisse_gunluk.parquet")
haftalık_ozet.to_parquet("../data/parquet/haftalık_ozet.parquet")



