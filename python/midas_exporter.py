from PyPDF2 import PdfReader
import re
import pandas as pd
import numpy as np
import time

def midas_exporter(filename):
    start = time.time()
    # midas1 = parser.from_file(filename)["content"]

    pdf = PdfReader(open(filename,'rb'))
    midas1 = ""
    for page in pdf.pages:
        midas1 += page.extract_text()
    yatırım_baslangic = midas1.find("YATIRIM İŞLEMLERİ")
    portfoy_ozet = midas1[:yatırım_baslangic]

    matches = re.findall(r"(\S+)\s+-\s+([^0-9]+)\s+(\d+)\s+([\d.,]+)\s+TRY\s+([\d.,-]+)\s+TRY\s+([\d.,]+)\s+TRY", portfoy_ozet)

    portfoy_df = pd.DataFrame(matches, columns=['Sermaya Piyasası Aracı', 'Company Name', 'Adet', 'Hisse Başı Ortalama Maliyet', 'Kar / Zarar*', 'Toplam Değeri (YP)'])

    
    for col in ['Adet', 'Hisse Başı Ortalama Maliyet', 'Kar / Zarar*', 'Toplam Değeri (YP)']:
        portfoy_df[col] = portfoy_df[col].str.replace(',', '.').astype(float)

    lines = midas1.strip().split("\n")

    pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) ((?:Piyasa|Limit) Emri) (\S+) (.*?) ((?:Gerçekleşti|Süresi Doldu|İptal Edildi)) (\S+) (\d+) (\S+) (\d+) ([\d.,]+|-) ([\d.,]+|-) ([\d.,]+)"

    data = []
    for line in lines[3:-2]:  
        match = re.search(pattern, line)
        if match:
            data.append(match.groups())

    investment_df = pd.DataFrame(data, columns=['Tarih', 'İşlem Türü', 'Sembol', 'İşlem Tipi', 'İşlem Durumu', 'Para Birimi', 'Emir Adedi', 'Emir Tutarı', 'Gerçekleşen Adet', 'Ortalama İşlem Fiyatı', 'İşlem Ücreti', 'İşlem Tutarı'])
    investment_df = investment_df[investment_df["İşlem Durumu"] == 'Gerçekleşti']
    for col in [ 'Emir Adedi', 'Gerçekleşen Adet', 'Ortalama İşlem Fiyatı', 'İşlem Ücreti', 'İşlem Tutarı']:
        investment_df[col] = investment_df[col].str.replace(',', '.').astype(float)


    investment_df['Tarih'] = pd.to_datetime(investment_df['Tarih'], format='%d/%m/%y %H:%M:%S')
    investment_df['Tarih'] = np.where(investment_df['Tarih'].dt.dayofweek == 5, investment_df['Tarih'] + pd.Timedelta(days=2), investment_df['Tarih'])
    investment_df['Tarih'] = np.where(investment_df['Tarih'].dt.dayofweek == 6, investment_df['Tarih'] + pd.Timedelta(days=1), investment_df['Tarih'])


    hesap_baslangic = midas1.find("HESAP İŞLEMLERİ")
    temettu_baslangic = midas1.find("TEMETTÜ İŞLEMLERİ")
    hesap_text = midas1[hesap_baslangic:temettu_baslangic]
    pattern = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (Para [\w\s]+?) ([\w\s-]+ - TR\d+) (\w+) ([\d,]+,\d{2} TRY)"

    matches = re.findall(pattern, hesap_text)

    hesap_df = pd.DataFrame(matches, columns=["Talep Tarihi", "İşlem Tarihi", "İşlem Tipi", "İşlem Açıklaması", "İşlem Durumu", "Tutar (YP)"])

    hesap_df["Tutar (YP)"] = hesap_df["Tutar (YP)"].str.replace(',', '.').str.extract('(\d+\.\d+)').astype(float)
    print(f"Time elapsed: {time.time() - start:.2f} seconds")
    return portfoy_df, investment_df, hesap_df