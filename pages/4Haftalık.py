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

st.markdown("<h1 style='text-align: center; color: white;'>Haftalık Özet</h1>", unsafe_allow_html=True)

# gunluk_ozet
# Extracting data
haftalık_dates = haftalık_ozet["date"].dt.strftime("%Y-%m-%d").tolist()
haftalık_dpy_values = [round(val, 2) for val in haftalık_ozet["d_p_y"].tolist()]

# Formatting series data for colors
haftalık_formatted_values = []
for val in haftalık_dpy_values:
    if val >= 0:
        haftalık_formatted_values.append(
            {"value": val, "itemStyle": {"color": "#4BD25B"}}
        )  # green for positive
    else:
        haftalık_formatted_values.append(
            {"value": val, "itemStyle": {"color": "#CF3A4B"}}
        )  # red for negative

# Updating options
options_bar_haftalık = {
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "shadow"},
    },
    "xAxis": {
        "type": "category",
        "data": haftalık_dates,
        "axisLabel": {
            "interval": len(haftalık_dates)
            // 6  # This will approximately display 6 dates on the x-axis
        },
    },
    "yAxis": {"type": "value"},
    "series": [
        {
            "data": haftalık_formatted_values,
            "type": "bar",
        }
    ],
}

st_echarts(
    options=options_bar_haftalık,
    height="400px",
)

haftalık_t_v_values = haftalık_ozet["t_v"].tolist()
haftalık_a_inv_values = haftalık_ozet["a_inv"].tolist()


options_portfoy = {
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
            "data": haftalık_dates,
        }
    ],
    "yAxis": [{"type": "value"}],
    "series": [
        {
            "name": "Portfolyo",
            "type": "line",
            "stack": "总量",
            "areaStyle": {},
            "emphasis": {"focus": "series"},
            "data": haftalık_t_v_values,
        },
        {
            "name": "Yatırım",
            "type": "line",
            "stack": "总量",
            "areaStyle": {},
            "emphasis": {"focus": "series"},
            "data": haftalık_a_inv_values,
        },
    ],
}

# st_echarts(options=options_portfoy, height="400px")