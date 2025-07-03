"""
   --------------------------------------------------------------------
   ---ETL para el histórico de precios del Bitcoin (últimos 30 días)---
   --------------------------------------------------------------------"""

#Importación de librerías y proceso de Extracción de datos (API pública de CoinGecko).

import requests 
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
params = {
    "vs_currency": "usd",
    "days": "30",
    "interval": "daily"
}

response = requests.get(url, params=params)
data = response.json()

print(response)
print(type(data))

#Proceso de Transformación y Limpieza de datos.

#Transformación de datos desde el tipo Diccionario a DatFrame
df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms")
df['price'] = df['price'].round(2)
df['daily_change'] = df['price'].pct_change().round(4)

#Eliminación de datos nulos en columnas; y duplicados en la columna de fecha ("timestamp")
df.dropna(inplace=True)
df.drop_duplicates(subset=['timestamp'], inplace=True)

print(df.info())
print(df)

#Visualización parcial de datos con libería Matplotlib y su modulo PyPlot

plt.figure(figsize=(10,4))
plt.plot(df['timestamp'], df['price'], color="orange")
plt.title("Bitcoin Price - Last 30 days")
plt.ylabel("Price in USD")
plt.xlabel("Date")
plt.xticks(rotation=50)
plt.tight_layout()
plt.savefig("../visuals/bitcoin_graphic.jpg")
plt.show()

#Carga de datos limpios hacia un archivo CSV o base de datos SQLite

df.to_csv("../data/processed/clean_bitcoin_data.csv")


#Creación de motor y base de datos SQLite llamada db_bitcoin.db
engine = create_engine('sqlite:///../data/processed/db_bitcoin.db', echo=False)

#Carga a la base de datos SQLite
df.to_sql('bitcoin_history', con=engine, if_exists='replace', index=False)
print("Datos cargados correctamente!")

#Lectura rápida de base de datos SQLite y archivo CSV

df_db = pd.read_sql("SELECT * FROM bitcoin_history;", con=engine)
print("SQLite: \n", df_db)

df_csv = pd.read_csv("../data/processed/clean_bitcoin_data.csv")
print("CSV: \n", df_csv)
