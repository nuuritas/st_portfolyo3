import pandas as pd

def excel_transformer(filename):
    data = pd.read_excel(filename)
    data.columns = data.columns.str.lower()
    data.rename(columns={'tarih': 'Tarih',
                        "sembol":"Sembol",
                        "tur":"İşlem Tipi",
                        "fiyat":"Ortalama İşlem Fiyatı",
                        "adet":"Gerçekleşen Adet"}, inplace=True)
    data["İşlem Tipi"] = data["İşlem Tipi"].str.replace("BUY", "Alış").str.replace("SELL", "Satış")

    return data