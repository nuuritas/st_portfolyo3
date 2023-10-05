import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_echarts import st_echarts

st.set_page_config(layout="wide")

gunluk_ozet = pd.read_parquet("data/parquet/gunluk_ozet.parquet")
haftalık_ozet = pd.read_parquet("data/parquet/haftalık_ozet.parquet")
port_all = pd.read_parquet("data/parquet/port_all.parquet")
hisse_gunluk = pd.read_parquet("data/parquet/hisse_gunluk.parquet")

from datetime import datetime, timedelta
now = datetime.now()
if now.weekday() >= 5:  # 5: Saturday, 6: Sunday
    days_to_subtract = now.weekday() - 4
    today = now.date() - timedelta(days=days_to_subtract)
    today_str = (now - timedelta(days=days_to_subtract)).strftime("%d-%m-%Y")
else:
    if now.hour < 18:
        today = now.date() - timedelta(days=1)
        today_str = (now - timedelta(days=1)).strftime("%d-%m-%Y")
    else:
        today = now.date()
        today_str = now.strftime("%d-%m-%Y")
        
tvdata = pd.read_parquet("data/parquet/data_daily.parquet")

st.title("Hisse Analizi")
st.header("Tek Hisse ile Alış-Satış & Kar ve Maliyet Analizi")
all_ticker = hisse_gunluk["ticker"]
tickers = sorted(set(all_ticker))

st.divider()
ind_options = st.selectbox("İncelemek için Hisse Seç", tickers)
tek_hisse = hisse_gunluk.query("ticker == @ind_options")

tablo = port_all.query("(ticker == @ind_options) & (d_q_b > 0 | d_q_s > 0)")
tablo2 = port_all.query("(ticker == @ind_options)")
tablo.sort_values(by="date", ascending=True, inplace=True)
tablo["date"] = tablo["date"].dt.strftime("%Y-%m-%d")
tablo.set_index("date", inplace=True)

m1, m2, m3, m4 = st.columns((1, 1, 1, 1))
last_col = tablo2.iloc[-1]

m1.metric(
    label="Güncel Fiyat",
    value=round(last_col["close"], 2),
    delta="%" + str(round(last_col["d_%"], 2)),
    delta_color="normal",
)
m2.metric(
    label="Maliyet (₺)",
    value=int(last_col["a_p_b"]),
)
m3.metric(
    label="Adet",
    value=int(last_col["h_q"]),
    delta=int(last_col["d_q_c"]),
    delta_color="normal",
)
m4.metric(
    label="K/Z",
    value=round(last_col["a_p"], 3),
    delta=str(round(last_col["a_%"], 2)) + "%",
    delta_color="normal",
)

# Extracting and formatting dates
dates_hisse = tek_hisse["date"].dt.strftime("%Y-%m-%d").tolist()
close_values = tek_hisse["close"].tolist()

# Extracting buy/sell data
buy_data = port_all.query("ticker == @ind_options & d_q_b > 0")
buy_dates = buy_data["date"].dt.strftime("%Y-%m-%d").tolist()
buy_prices = buy_data["d_p_b"].tolist()
buy_quantities = buy_data["d_q_b"].tolist()

sell_data = port_all.query("ticker == @ind_options & d_q_s > 0")
sell_dates = sell_data["date"].dt.strftime("%Y-%m-%d").tolist()
sell_prices = sell_data["d_p_s"].tolist()
sell_quantities = sell_data["d_q_s"].tolist()

# Constructing tooltip data for scatter points
scatter_data_buy = [
    [date, price, quantity]
    for date, price, quantity in zip(buy_dates, buy_prices, buy_quantities)
]

scatter_data_sell = [
    [date, price, quantity]
    for date, price, quantity in zip(sell_dates, sell_prices, sell_quantities)
]

option = {
    "xAxis": {"type": "category", "data": dates_hisse,
              "axisLine": {"lineStyle": {"color": "#ffffff"}}},
    "yAxis": {"type": "value", "scale": True, "splitLine": {"show": False},
              "axisLabel": {"formatter": "{value} ₺"},
              "axisPointer": {"label": {"formatter": "{value} ₺"}},
              "axisLine": {"lineStyle": {"color": "#ffffff"}}},

    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "cross", "label": {"backgroundColor": "#6a7985"}},
    },
    "series": [
        {
            "data": close_values,
            "name": "Fiyat",
            "type": "line",
            "smooth": True,
            "tooltip": {"formatter": "Date: {b} <br> Close: {c}"},
        },
        {
            "name": "Alış",
            "type": "scatter",
            "coordinateSystem": "cartesian2d",
            "data": scatter_data_buy,
            "symbolSize": 20,
            "symbolColor": "green",
            "label": {
                "show": True,
                "formatter": "{@[2]}",
                "position": "top",
                "color": "#ffffff",
            },
            "symbol": "pin",
            "itemStyle": {"color": "#CEFF85"},
        },
        {
            "name": "Satış",
            "type": "scatter",
            "coordinateSystem": "cartesian2d",
            "data": scatter_data_sell,
            "symbolSize": 20,
            "symbolColor": "red",
            "label": {
                "show": True,
                "formatter": "{@[2]}",
                "position": "top",
                "color": "white",
            },
            "symbol": "pin",
            "itemStyle": {"color": "red"},

        },
    ],
}

st_echarts(options=option, height="400px")

st.subheader("Alış/Satış Tablosu")

tablo.rename(
    columns={
        "d_q_b": "Q-Al",
        "d_q_s": "Q-Sat",
        "d_p_b": "P-Al",
        "d_p_s": "P-Sat",
        "close": "P-Close",
    },
    inplace=True,
)
st.dataframe(tablo[["Q-Al", "Q-Sat", "P-Al", "P-Sat", "P-Close"]])
# tablo[["date","ticker","d_q_b","d_q_s","d_p_b","d_p_s","close"]]
