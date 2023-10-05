from numpy import empty
import streamlit as st
import sys
sys.path.append('../')
from python.midas_exporter import midas_exporter
from python.midas_raw_creator import midas_raw_creator

import tempfile

filename = "midas_8.pdf"

uploaded_file = st.file_uploader("Choose a file",accept_multiple_files=True,type=['pdf'])

if uploaded_file:
    portfoy_files = []; investment_files = []; hesap_files = []
    for file in uploaded_file:
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            # Save the uploaded file's content to the temporary file
            temp_file.write(file.getvalue())
            # Ensure the data is flushed to the file
            temp_file.flush()

            # Now you can use temp_file.name as the filename for your function
            # midas1 = parser.from_file(temp_file.name)["content"]
            portfoy_df, investment_df, hesap_df = midas_exporter(temp_file.name)
            portfoy_files.append(portfoy_df); investment_files.append(investment_df); hesap_files.append(hesap_df)

    investment_df, cum_inv_df = midas_raw_creator(portfoy_files, investment_files, hesap_files)

    st.dataframe(investment_df)



