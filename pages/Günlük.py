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

tvdata = pd.read_parquet("data/parquet/tvdata23.parquet")
index = tvdata.query("ticker == 'XU100'").reset_index(drop=True)
del tvdata
index["change"] = (index["close"] / index["open"] - 1) * 100
index["date"] = index["date"].apply(lambda x: x.normalize())

st.title("Günlük Bazda Sonuçlar")
# st.header("Burada günlük kazanç ve portfolyo değişimi yer alır.")

# gunluk_ozet
# Extracting data
dates = gunluk_ozet["date"].dt.strftime("%Y-%m-%d").tolist()
values = [round(val, 2) for val in gunluk_ozet["d_p_y"].tolist()]
index_values = [
    round(val, 2) for val in index.query("date == @dates")["change"].tolist()
]


formatted_values = []
for val in values:
    if val >= 0:
        formatted_values.append(
            {"value": val, "itemStyle": {"color": "#2DE1C2"}}
        )  # green for positive
    else:
        formatted_values.append(
            {"value": val, "itemStyle": {"color": "red"}}
        )  # red for negative

# Updating options
options_bar_gunluk = {
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "shadow"},
    },
    "title": {
        "text": "Günlük Getiri",
        "left": "center",
        "textStyle": {"color": "#ffffff"},
    },
    "xAxis": {
        "type": "category",
        "data": dates,
        "axisLabel": {
            "interval": len(dates)
            // 6  # This will approximately display 6 dates on the x-axis
        },
        "axisLine": {"lineStyle": {"color": "#ffffff"}},
    },
    "yAxis": {"type": "value", "axisLine": {"lineStyle": {"color": "#ffffff"}},
              "splitLine": {"show": False}},
    "series": [
        {
            "data": formatted_values,
            "name": "Günlük(%)",
            "type": "line",
            "color": "#42c8b2",
            "smooth": False,
            "itemStyle": {"color": "#42c8b2"},
            "lineStyle": {"color": "#42c8b2", "type": "solid", "width": 3},
            "order": "before",
        },
        {
            "data": index_values,
            "name": "XU100(%)",
            "type": "line",
            "smooth": False,
            "color": "white",
            "itemStyle": {"color": "white"},
            "lineStyle": {"color": "white", "type": "dashed", "width": 1},
            "order": "before",
        },
    ],
    "dataZoom": [
        {
            "type": "inside",
            "start": 0,
            "end": 100,
        }
    ],
}



st_echarts(
    options=options_bar_gunluk,
    height="400px",
)

t_v_values = gunluk_ozet["t_v"].tolist()
a_inv_values = gunluk_ozet["a_inv"].tolist()

# st.dataframe(gunluk_ozet)
options_portfoy_gunluk = {
    "title": {"text": "Portföy ve Yatırım", "textStyle": {"color": "#ffffff"}},
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "cross", "label": {"backgroundColor": "#6a7985"}},
    },
    "legend": {
        "data": ["Portfolyo", "Yatırım"],
        "textStyle": {"color": "#ffffff "},
    },
    "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
    "xAxis": [
        {
            "type": "category",
            "boundaryGap": False,
            "data": dates,
            "axisLine": {"lineStyle": {"color": "#ffffff"}},
        }
    ],
    "yAxis": [{"type": "value", "axisLine": {"lineStyle": {"color": "#ffffff"}}}],
    "series": [
        {
            "name": "Portfolyo",
            "type": "line",
            "areaStyle": {},
            "emphasis": {"focus": "series"},
            "data": t_v_values,
        },
        {
            "name": "Yatırım",
            "type": "line",
            "areaStyle": {},
            "emphasis": {"focus": "series"},
            "data": a_inv_values,
        },
    ],
}

# st_echarts(options=options_portfoy_gunluk, height="400px")
