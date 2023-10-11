import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_echarts import st_echarts

# st.set_page_config(layout="wide")

gunluk_ozet = pd.read_parquet("data/parquet/gunluk_ozet.parquet")
haftalık_ozet = pd.read_parquet("data/parquet/haftalık_ozet.parquet")
port_all = pd.read_parquet("data/parquet/port_all.parquet")

wch_colour_font = (255, 255, 255)
fontsize = 24
lnk = '<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.12.1/css/all.css" crossorigin="anonymous">'


def main_holdings_html(
    total_value,
    days_gain,
    days_gain_perc,
    total_gain,
    total_gain_perc,
    days_gain_color,
    total_gain_color,
):
    current_date = datetime.now().strftime("%d-%m-%Y")
    return f"""
    <link href="https://fonts.googleapis.com/css2?family=Lilita+One&display=swap" rel="stylesheet">
    <div style='background-color: transparent; color: white; border-radius: 7px; padding: 12px; line-height: 25px; border: 1px solid white; margin-bottom: 12px;font-family: "Lilita One", cursive;'>
        <span style='font-size: 22px; display: block;'>BAKIYE</span>
        <span style='font-size: 28px; display: block;'>{total_value} TL</span>
        <br>
        <div style='display: flex; justify-content: space-between;'>
            <span style='font-size: 18px; display: block;'>GÜNLÜK KZ:</span>
            <span style='font-size: 20px; color: {days_gain_color}; display: block;'>{days_gain} TL(%{days_gain_perc})</span>
        </div>
        <div style='display: flex; justify-content: space-between;'>
            <span style='font-size: 18px; display: block;'>TOPLAM KZ:</span>
            <span style='font-size: 20px; color: {total_gain_color}; display: block;'>{total_gain} TL(%{total_gain_perc})</span>
        </div>
        <span style='font-size: 14px; display: block; color:yellowq'>Güncelleme: {current_date}</span>
    </div>
    """


def generate_metric_html(label, value, delta):
    # Determine color for delta value
    color, arrow, arrow2 = ("#4BD25B", "↑", "") if delta >= 0 else ("red", "↓", "")

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
            font-size: 16px;
            font-weight: bold;
            font-family: 'Lilita One', cursive;
        }
        .value {
            font-family: 'Lilita One', cursive;
            font-size: 24px;
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

hisse_gunluk = pd.read_parquet("data/parquet/hisse_gunluk.parquet")

# st.dataframe(gunluk_ozet)
# st.title(f"{datetime.today().strftime('%d-%m-%Y')} Özet")


toplam_buyukluk = port_all.query("date == @today").t_v.sum()
toplam_net = round(
    gunluk_ozet.query("date == @today")["a_ur_p"].values[0]
    + gunluk_ozet.query("date == @today")["a_r_p"].values[0]
)
toplam_yuzde = round(toplam_net / toplam_buyukluk * 100, 1)
gunluk_net = gunluk_ozet.query("date == @today").d_p.values[0]
gunluk_yuzde = gunluk_ozet.query("date == @today").d_p_y.values[0]

gunluk_ozet["week"] = gunluk_ozet["date"].dt.isocalendar().week
gunluk_ozet["month"] = gunluk_ozet["date"].dt.month
thisweek = today.isocalendar().week
lastweek = thisweek - 1
thismonth = today.month

son_hafta = gunluk_ozet.query("week == @thisweek")
son_hafta.reset_index(drop=True, inplace=True)
haftalik_net = (
    son_hafta.iloc[-1]["t_v"] - son_hafta.iloc[0]["t_v"] - son_hafta["d_inv"].sum()
)
haftalik_yuzde = round(
    (1 - (son_hafta.iloc[0]["t_v"] / son_hafta.iloc[-1]["t_v"])) * 100, 2
)

son_ay = gunluk_ozet.query("month == @thismonth")
son_ay.reset_index(drop=True, inplace=True)
aylik_net = son_ay.iloc[-1]["t_v"] - son_ay.iloc[0]["t_v"] - son_ay["d_inv"].sum()
aylik_yuzde = round(
    (1 - (son_ay.iloc[0]["t_v"] / (son_ay.iloc[-1]["t_v"] - son_ay["d_inv"].sum())))
    * 100,
    2,
)

son_gun = hisse_gunluk.query("date == @today").sort_values(
    by="t_v", ascending=True
)
son_gun.dropna(how="any", inplace=True)
data_list = [
    {"name": ticker, "value": round(value, 1)}
    for ticker, value in son_gun[["ticker", "t_v"]].values
]

# st.subheader(f"Portfolyo Büyüklüğü: {int(toplam_buyukluk)}₺")

main_html = main_holdings_html(
    total_value=int(toplam_buyukluk),
    days_gain=int(gunluk_net),
    days_gain_perc=round(gunluk_yuzde, 1),
    total_gain=toplam_net,
    total_gain_perc=toplam_yuzde,
    days_gain_color="#4BD25B" if gunluk_net >= 0 else "#CF3A4B",
    total_gain_color="#4BD25B" if toplam_net >= 0 else "#CF3A4B",
)


st.markdown(lnk + main_html, unsafe_allow_html=True)

options_pie_main = {
    "tooltip": {"trigger": "item", "formatter": "{c}₺ <br>  (%{d})"},
    "series": [
        {
            "name": "Hisse",
            "type": "pie",
            "radius": ["30%", "45%"],
            "center": ["50%", "45%"],
            "avoidLabelOverlap": "false",
            "data": [],
            "label": {
                "color": "#ffffff",
                "fontSize": 14,
                "fontWeight": "bold",
            },
            "emphasis": {
                "label": {
                    "show": "true",
                    "fontWeight": "bold",
                    "fontSize": 16,
                    "fontColor": "#ffffff",
                    
                },
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowOffsetX": 0,
                    "shadowColor": "rgba(0, 0, 0, 0.5)",
                },
                "focus": "self",
            },
        }
    ],
}


options_pie_main["series"][0]["data"] = data_list

st_echarts(
    options=options_pie_main,
    height="300px",
)




metrics = [
    ("Günlük(%)", gunluk_yuzde, round(gunluk_net)),
    ("Haftalık(%)", haftalik_yuzde, round(haftalik_net)),
    ("Aylık(%)", aylik_yuzde, round(aylik_net)),
]

st.markdown(generate_metrics_html(metrics), unsafe_allow_html=True)
