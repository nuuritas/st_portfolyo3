import pandas as pd

def excel_transformer(filename):
    data = pd.read_excel(filename)
    data.columns = data.columns.str.lower()
    data.rename(columns={'tarih': 'date',
                        "sembol":"ticker",
                        "tur":"buy_sell",
                        "fiyat":"price",
                        "adet":"quantity"}, inplace=True)
    data["buy_sell"] = data["buy_sell"].str.replace("BUY", "Alış").str.replace("SELL", "Satış")

    return data