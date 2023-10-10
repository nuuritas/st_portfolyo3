import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_echarts import st_echarts
from datetime import datetime, timedelta

st.set_page_config(layout="wide")

gunluk_ozet = pd.read_parquet("data/parquet/gunluk_ozet.parquet")
haftalık_ozet = pd.read_parquet("data/parquet/haftalık_ozet.parquet")
port_all = pd.read_parquet("data/parquet/port_all.parquet")
hisse_gunluk = pd.read_parquet("data/parquet/hisse_gunluk.parquet")



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
index = tvdata.query("ticker == 'XU100'").reset_index(drop=True)
del tvdata
index["change"] = (index["close"] / index["open"] - 1) * 100
index["date"] = index["date"].apply(lambda x: x.normalize())

# st.title("Günlük Özet")
st.markdown("<h1 style='text-align: center; color: white;'>Günlük Özet</h1>", unsafe_allow_html=True)


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
            {"value": val, "itemStyle": {"color": "#4BD25B"}}
        )  # green for positive
    else:
        formatted_values.append(
            {"value": val, "itemStyle": {"color": "#CF3A4B"}}
        )  # red for negative

# Updating options
options_bar_gunluk = {
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "shadow"},
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
            "type": "bar",
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
            "smooth": True,
            "color": "white",
            "itemStyle": {"color": "white"},
            "lineStyle": {"color": "white", "type": "dashed", "width": 1, "opacity": 0.8},
            "order": "before",
            "showSymbol": False
        },
    ],
    "dataZoom": [
        {
            "type": "inside",
            "start": 0,
            "end": 100,
        }
    ],
    "legend": {
        "data": ["Günlük(%)", "XU100(%)"],
        "textStyle": {"color": "#ffffff"},
        "top": "5%",
        "left": "center",
    },
}



st_echarts(
    options=options_bar_gunluk,
    height="400px",
)

t_v_values = gunluk_ozet["t_v"].tolist()
a_inv_values = gunluk_ozet["a_inv"].tolist()

# st.dataframe(gunluk_ozet)
options_portfoy_gunluk = {
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
            "areaStyle": {
                "color": {
                    "type": "linear",
                    "x": 0,
                    "y": 0,
                    "x2": 0,
                    "y2": 1,
                    "colorStops": [
                        {"offset": 0, "color": "#D4F2D2"},
                        {"offset": 1, "color": "#D4F2D2"},
                    ],
                    "global": False,
                }   
            },
            "color": "#D4F2D2",
            "emphasis": {"focus": "series"},
            "data": t_v_values,
            "showSymbol": True,
            "symbol": "circle",
        },
        {
            "name": "Yatırım",
            "type": "line",
            "emphasis": {"focus": "series"},
            "color": "#CF3A4B",
            "data": a_inv_values,
            "symbol": "circle",
            "showSymbol": True,
        },
    ],
}

st_echarts(options=options_portfoy_gunluk, height="400px")
