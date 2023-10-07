import pandas as pd
import streamlit as st

data = pd.read_parquet("data/midas_raw/midas_df.parquet")

st.data_editor(data)