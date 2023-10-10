import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
# st.set_page_config(layout="centered")

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

port_all = pd.read_parquet("data/parquet/port_all.parquet")
port_all_today = port_all.query("date == @today")
port_all_today.dropna(how="any", inplace=True)
port_all_today.query("t_v > 0", inplace=True)
tvdata = pd.read_parquet("data/parquet/data_daily.parquet")

wch_colour_font = (255, 255, 255)
fontsize = 24
lnk = '<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.12.1/css/all.css" crossorigin="anonymous">'

st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Rubik+Mono+One&display=swap" rel="stylesheet">
    <style>
        .rubik-text {
            font-family: 'Rubik Mono One', sans-serif;
            font-size: 24px;  /* adjust as needed */
            text-align: center;
        }
    </style>
    <div class="rubik-text">Hisseler</div>
""", unsafe_allow_html=True)
st.divider()


def generate_sparkline(values, color="white"):
    """Generate an SVG sparkline from a list of values."""
    # Normalize values to fit within SVG viewBox
    max_val = max(values)
    min_val = min(values)

    height = 30
    width = 100

    normalized_values = [
        (val - min_val) / (max_val - min_val) * height for val in values
    ]
    # print(normalized_values)
    path_data = "M 0," + str(height - normalized_values[0])  # Move to starting point

    # Create path data from normalized values
    for i, val in enumerate(normalized_values, start=1):
        path_data += f" L {i * (width / (len(values)))},{height - val}"

    # Check if the last value is above or below the start value to decide the color of the sparkline
    if normalized_values[-1] > normalized_values[0]:
        color = "green"
    else:
        color = "red"

    # Return SVG string
    return f"""
    <svg width="100%" height="100%" viewBox="0 0 100 30" xmlns="http://www.w3.org/2000/svg">
        <line x1="0" y1="{height - normalized_values[0]}" x2="100" y2="{height - normalized_values[0]}" stroke="white" stroke-dasharray="2,2" stroke-width="1"/>
        <path d="{path_data}" fill="none" stroke="{color}" stroke-width="1.5"/>
    </svg>
    """

def stock_html(stock_code, ticker_price,sparkline_svg, holding_value, daily_gain, daily_gain_perc,gain_color):
    return f"""
    <link href="https://fonts.googleapis.com/css2?family=Lilita+One&display=swap" rel="stylesheet">
    <div style='display: flex; justify-content: space-between; align-items: center;
                background-color: transparent; font-family: "Lilita One", cursive;
                border-radius: 10px; padding: 12px; line-height: 25px; border: 1px solid white;'>
        <div style='flex: 1; text-align: left;'> 
            <span style='font-size: {fontsize-2}px; '>{stock_code}</span>
            <span style='font-size: 18px; display: block;'>TL{round(ticker_price,2)}</span>
        </div>
        <div style='flex: 1;'>{sparkline_svg}</div>
        <div style='flex: 1; text-align: right;'>
            <span style='font-size: {fontsize-2}px; display: block;'>{holding_value} TL</span>
            <span style='font-size: 14px; color: {gain_color}; display: block;'>{daily_gain}TL(%{daily_gain_perc})</span>
        </div>
    </div>
    """


# Main Holdings Info Box
def main_holdings_html(
    total_value,
    days_gain,
    days_gain_perc,
    total_gain,
    total_gain_perc,
    days_gain_color,
    total_gain_color,
):
    current_date = datetime.now().strftime("%B %d, %Y")
    return f"""
    <link href="https://fonts.googleapis.com/css2?family=Lilita+One&display=swap" rel="stylesheet">
    <div style='background-color: transparent; color: white; border-radius: 7px; padding: 12px; line-height: 25px; border: 1px solid white; margin-bottom: 12px;font-family: "Lilita One", cursive;'>
        <span style='font-size: 22px; display: block;'>BAKİYE</span>
        <span style='font-size: 28px; display: block;'>₺{total_value}</span>
        <br>
        <div style='display: flex; justify-content: space-between;'>
            <span style='font-size: 16px; display: block;'>Günlük Kazanç:</span>
            <span style='font-size: 20px; color: {days_gain_color}; display: block;'>${days_gain}({days_gain_perc})</span>
        </div>
        <div style='display: flex; justify-content: space-between;'>
            <span style='font-size: 16px; display: block;'>Toplam Kazanç:</span>
            <span style='font-size: 20px; color: {total_gain_color}; display: block;'>${total_gain}({total_gain_perc})</span>
        </div>
        <span style='font-size: 12px; display: block; color:lightgrey'>{current_date}</span>
    </div>
    """


main_html = main_holdings_html("5000", "75", "2","-50", "-3", "green", "red")

for index, row in port_all_today.sort_values("t_v",ascending=False).iterrows():
    ticker = row['ticker']
    holding_value = int(row['t_v'])
    daily_gain = int(row['d_p'])
    daily_gain_perc = round(row['d_%'],2)

    gain_color = "#4BD25B" if daily_gain >= 0 else "#CF3A4B"
    
    # Fetching last 10 close values for the ticker from tvdata
    spark_values = tvdata.query("ticker == @ticker")['close'].tail(10).tolist()
    ticker_price = spark_values[-1]
    # print(ticker,spark_values)
    spark_svg = generate_sparkline(spark_values)
    
    # Generating the HTML
    stock_html_content = stock_html(ticker, ticker_price, spark_svg, holding_value, daily_gain,daily_gain_perc, gain_color)
    st.markdown(stock_html_content, unsafe_allow_html=True)