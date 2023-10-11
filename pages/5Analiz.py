import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_echarts import st_echarts
import warnings; warnings.simplefilter("ignore")

st.set_page_config(layout="wide")

gunluk_ozet = pd.read_parquet("data/parquet/gunluk_ozet.parquet")
haftalık_ozet = pd.read_parquet("data/parquet/haftalık_ozet.parquet")
port_all = pd.read_parquet("data/parquet/port_all.parquet")
hisse_gunluk = pd.read_parquet("data/parquet/hisse_gunluk.parquet")
price_data = pd.read_parquet("data/parquet/data_daily.parquet")



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


def generate_metric_html(label, value, delta):
    # Determine color for delta value
    if delta > 0:
        color, arrow, arrow2 = "#4BD25B", "↑", ""
    elif delta < 0:
        color, arrow, arrow2 = "#CF3A4B", "↓", ""
    else:
        color, arrow, arrow2 = "#FFFFFF", "", ""  # Choose appropriate color and symbols for the zero case

    # Create the HTML structure
    html = f"""
    <div class="metric">
        <div class="label">{label}</div>
        <div class="value" style="color: {color};">{arrow2} {value}</div>
        <div class="delta" style="color: {color};">{arrow} {delta} TL</div>
    </div>
    """
    return html

def generate_metrics_html(metrics):
    html_header = """
    <link href="https://fonts.googleapis.com/css2?family=Lilita+One&display=swap" rel="stylesheet">
    <style>
        .metrics-container {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .metric {
            flex: 1;
            text-align: center;
            border: 1px solid #ccc;
            padding: 10px;
            border-radius: 5px;
        }
        .label {
            font-size: 14px;
            font-weight: bold;
            font-family: 'Lilita One', cursive;
        }
        .value {
            font-size: 24px;
            font-family: 'Lilita One', cursive;
        }
        .delta {
            font-size: 16px;
            font-family: 'Lilita One', cursive;
        }
    </style>
    <div class="metrics-container">
    """

    html_footer = """
    </div>
    """

    metrics_html = [
        generate_metric_html(label, value, delta) for label, value, delta in metrics
    ]
    final_html = html_header + "".join(metrics_html) + html_footer

    return final_html


st.markdown("<h1 style='text-align: center; color: white;'>Hisse Analiz</h1>", unsafe_allow_html=True)
all_ticker = hisse_gunluk["ticker"]
tickers = sorted(set(all_ticker))
today_tickers = sorted(hisse_gunluk.query("date == @today & h_q > 0")["ticker"].tolist())


st.divider()
today_option = st.checkbox("Güncel Hisseler", value=True)
if today_option:
    ind_options = st.selectbox("İncelemek için Hisse Seç", today_tickers)
else:
    ind_options = st.selectbox("İncelemek için Hisse Seç", tickers, index=0)
tek_hisse = hisse_gunluk.query("ticker == @ind_options")

tablo = port_all.query("(ticker == @ind_options) & (d_q_b > 0 | d_q_s > 0)")
tablo2 = port_all.query("(ticker == @ind_options)")
tablo.sort_values(by="date", ascending=True, inplace=True)
tablo["date"] = tablo["date"].dt.strftime("%Y-%m-%d")
tablo.set_index("date", inplace=True)
last_col = tablo2.iloc[-1]

metrics = [
    ("FIYAT", round(last_col["close"], 2), round(last_col["d_%"], 2)),
    ("MALIYET", int(last_col["a_p_b"]), 0),
    ("ADET", int(last_col["h_q"]), int(last_col["d_q_c"])),
    ("K/Z", round(last_col["a_p"]), round(last_col["a_%"], 2)),
]

st.markdown(generate_metrics_html(metrics), unsafe_allow_html=True)

first_day = tek_hisse["date"].iloc[0]
close_values = price_data.query("ticker == @ind_options & date >= @first_day")["close"].tolist()
dates_hisse = price_data.query("ticker == @ind_options & date >= @first_day")["date"].dt.strftime("%Y-%m-%d").tolist()


buy_data = port_all.query("ticker == @ind_options & d_q_b > 0")
buy_dates = buy_data["date"].dt.strftime("%Y-%m-%d").tolist()
buy_prices = buy_data["d_p_b"].tolist()
buy_quantities = buy_data["d_q_b"].tolist()

sell_data = port_all.query("ticker == @ind_options & d_q_s > 0")
sell_dates = sell_data["date"].dt.strftime("%Y-%m-%d").tolist()
sell_prices = sell_data["d_p_s"].tolist()
sell_quantities = sell_data["d_q_s"].tolist()

scatter_data_buy = [
    [date, price, quantity]
    for date, price, quantity in zip(buy_dates, buy_prices, buy_quantities)
]

scatter_data_sell = [
    [date, price, quantity]
    for date, price, quantity in zip(sell_dates, sell_prices, sell_quantities)
]

option = {
    "xAxis": {
        "type": "category",
        "data": dates_hisse,
        "axisLine": {"lineStyle": {"color": "#ffffff"}},
    },
    "yAxis": {
        "type": "value",
        "scale": True,
        "splitLine": {"show": False},
        "axisLabel": {"formatter": "{value} ₺"},
        "axisPointer": {"label": {"formatter": "{value} ₺"}},
        "axisLine": {"lineStyle": {"color": "#ffffff"}},
    },
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "shadow", "label": {"backgroundColor": "#6a7985"}},
    },
    "series": [
        {
            "data": close_values,
            "name": "Fiyat",
            "type": "line",
            "smooth": False,
            "tooltip": {"formatter": "Date: {b} <br> Close: {c}"},
            "color": "#11a4d6",
            "showSymbol": False,
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
            "itemStyle": {"color": "#4BD25B"},
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
            "itemStyle": {"color": "#CF3A4B"},
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
