import pandas as pd
import random
import streamlit as st
from streamlit_echarts import st_echarts

gunluk_ozet = pd.read_parquet("data/parquet/gunluk_ozet.parquet")
gunluk_ozet["date"] = gunluk_ozet["date"].dt.strftime("%Y-%m-%d")
# st.data_editor(gunluk_ozet)
gunluk_ozet["d_p_y"] = gunluk_ozet["d_p_y"].round(2)
data_list = gunluk_ozet[['date', 'd_p_y']].values.tolist()

def get_virtual_data(year, month):
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month % 12 + 1:02d}-01" if month != 12 else f"{year+1}-01-01"
    date_list = pd.date_range(start=start_date, end=end_date)
    return [[d.strftime("%Y-%m-%d"), random.randint(1, 10000)] for d in date_list]

option = {
    "tooltip": {"position": "top"},
    "visualMap": {
        "min": gunluk_ozet['d_p_y'].min(),
        "max": gunluk_ozet['d_p_y'].max(),
        "calculable": True,
        "orient": "vertical",
        "left": "%20",
        "top": "top",
        "textStyle": {"color": "#ffffff"},
        "inRange": {
            "color": ["red","white","darkgreen"]
        },
    },
    "calendar": [{
        "orient": "vertical",
        "range": "2023-09",
        "cellSize": ["auto", 20],
        "top": "20%",
        "dayLabel": {
            "nameMap": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
            "color": "#ffffff"
        },
        "monthLabel": {
            "nameMap": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "TMM", "AUG", "EYL", "EKM","KSM","Dec"],
            "color": "#ffffff",
            "fontWeight": "bold",
            "fontSize": 18
        },
        "itemStyle": {
            "borderWidth": 0.5
        },
        "splitLine": {
            "show": True,
            "lineStyle": {
                "color": "#ffffff",
                "width": 4,
                "type": "solid"
            }
        },
        "itemStyle": {
            "color": "#323c48",
            "borderWidth": 1,
            "borderColor": "#111"
        },
    }],
    "series": [{
        "type": "heatmap",
        "coordinateSystem": "calendar",
        "calendarIndex": 0,
        "data": data_list,
    }],
}

st_echarts(option, height="15em",width="80%")

option2 = {
    "tooltip": {"position": "top"},
    "visualMap": {
        "min": gunluk_ozet['d_p_y'].min(),
        "max": gunluk_ozet['d_p_y'].max(),
        "calculable": True,
        "orient": "vertical",
        "left": "%80",
        "top": "top",
        "textStyle": {"color": "#ffffff"},
        "inRange": {
          "color": ["red","white","darkgreen"]
        },
    },
    "calendar": [{
        "orient": "vertical",
        "range": "2023-08",
        "cellSize": ["auto", 20],
        "top": "20%",
        "monthLabel": {
            "nameMap": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "TMM", "AUG", "EYL", "EKM","KSM","Dec"],
            "color": "#ffffff",
            "fontWeight": "bold",
            "fontSize": 18
        },
        "dayLabel": {
            "nameMap": ["Pzr", "Pzt", "Salı", "Çrş", "Prş", "Cum", "Cmt"],
            "firstDay": 1,
            "color": "#ffffff"
        },
        "itemStyle": {
            "borderWidth": 0.5
        },
        "splitLine": {
            "show": True,
            "lineStyle": {
                "color": "#ffffff",
                "width": 4,
                "type": "solid"
            }
        },
        "itemStyle": {
            "color": "#323c48",
            "borderWidth": 1,
            "borderColor": "#111"
        },


    }],
    "series": [{
        "type": "heatmap",
        "coordinateSystem": "calendar",
        "calendarIndex": 0,
        "data": data_list,
    }],
}

st_echarts(option2, height="15em",width="80%")

option3 = {
    "tooltip": {"position": "top"},
    "visualMap": {
        "min": gunluk_ozet['d_p_y'].min(),
        "max": gunluk_ozet['d_p_y'].max(),
        "calculable": True,
        "orient": "vertical",
        "left": "%80",
        "top": "top",
        "textStyle": {"color": "#ffffff"},
        "inRange": {
            "color": ["red","white","darkgreen"]
        },
    },
    "calendar": [{
        "orient": "vertical",
        "range": "2023-07",
        "cellSize": ["auto", 20],
        "top": "20%",
        "monthLabel": {
            "nameMap": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "TMM", "AUG", "EYL", "EKM","KSM","Dec"],
            "color": "#ffffff",
            "fontWeight": "bold",
            "fontSize": 18
        },
        "dayLabel": {
            "nameMap": ["Pzr", "Pzt", "Salı", "Çrş", "Prş", "Cum", "Cmt"],
            "firstDay": 1,
            "color": "#ffffff"
        },
        "itemStyle": {
            "borderWidth": 0.5
        },
        "splitLine": {
            "show": True,
            "lineStyle": {
                "color": "#ffffff",
                "width": 4,
                "type": "solid"
            }
        },
        "itemStyle": {
            "color": "#323c48",
            "borderWidth": 1,
            "borderColor": "#111"
        },


    }],
    "series": [{
        "type": "heatmap",
        "coordinateSystem": "calendar",
        "calendarIndex": 0,
        "data": data_list,
    }],
}

st_echarts(option3, height="15em",width="80%")
