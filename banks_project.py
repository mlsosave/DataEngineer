# ETL operations on Largest Banks

"""
Full ETL script for Largest Banks Market Capitalization project
"""

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_csv_path = "./exchange_rate.csv"
table_attribs = ["Name","MC_USD_Billion"]
table_attribs_final = ["Name","MC_USD_Billion","MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"]
output_csv_path = "./Largest_banks_data.csv"
dbname = "Banks.db"
tablename = "Largest_banks"
logfile = "./code_log.txt"

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import requests
import sqlite3
import numpy as np

# -------------------- Logging --------------------
def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(logfile,"a") as f:
        f.write(timestamp + ' : ' + message + '\n')    

# -------------------- Extraction --------------------
def extract(url, table_attribs):
    log_progress('Preliminaries complete. Initiating ETL process')

    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page,'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')  # First table
    
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            gdp_text = cols[2].get_text(strip=True).replace(",", "").replace("\n","")
            if gdp_text != '—':
                data_dict = {
                    "Name": cols[1].get_text(strip=True),
                    "MC_USD_Billion": float(gdp_text)
                }
                df = pd.concat([df, pd.DataFrame([data_dict])], ignore_index=True)

    log_progress("Extraction completed")
    return df

# -------------------- Transformation --------------------
def transform(df, exchange_csv_path):

    # Ensure exchange_rate['GBP'] is always a float
    # gbp_rate = float(exchange_rate['GBP'])
    # df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]

    # dict = dataframe.set_index('Col_1_header').to_dict()['Col_2_header']
    
    log_progress('Initiating TRANSFORM process')

    # Read CSV and convert to dictionary
    df_exchange = pd.read_csv(exchange_csv_path)
    
    """
    zip(df_exchange['Currency'], df_exchange['Rate']) combina las dos columnas en pares: ("EUR", 0.93), ("GBP", 0.8), etc.
    dict(...) convierte esos pares en un diccionario:
    exchange_rate = {
       'EUR': 0.93,
       'GBP': 0.8,
       'INR': 82.95
    }
    """
    exchange_rate = dict(zip(df_exchange['Currency'], df_exchange['Rate']))
    
    """
    df['MC_GBP_Billion'] = [np.round(x * float(exchange_rate['GBP']), 2) for x in df['MC_USD_Billion']]
    for x in df['MC_USD_Billion'] → recorre todos los valores en la columna MC_USD_Billion.
    x * float(exchange_rate['GBP']) → multiplica cada valor por la tasa de conversión correspondiente.
    np.round(..., 2) → redondea cada resultado a 2 decimales.
    [ ... for x in ... ] → list comprehension, que genera la lista de todos los valores convertidos.
    Finalmente, esa lista se asigna a la nueva columna MC_GBP_Billion.
    """
    
    # Create new columns using list comprehension and round to 2 decimals
    df['MC_GBP_Billion'] = [np.round(x * float(exchange_rate['GBP']), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * float(exchange_rate['EUR']), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * float(exchange_rate['INR']), 2) for x in df['MC_USD_Billion']]

    df = df[table_attribs_final]

    log_progress('Finalizing TRANSFORM process')
    return df

# -------------------- Load to CSV --------------------
def load_to_csv(df, output_path):
    log_progress('Load to CSV process')
    df.to_csv(output_path, index=False)
    log_progress('Load to CSV process finalized')

# -------------------- Load to Database --------------------
def load_to_db(df, conn, table_name):
    log_progress('Start load to DB process')
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress('End load to DB process')

# -------------------- Run Queries --------------------
def run_query(query_statement, conn):
    log_progress(query_statement)
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)
    log_progress(query_output.to_string())

# -------------------- Main ETL flow --------------------
df = extract(url, table_attribs)
df = transform(df, exchange_csv_path)
load_to_csv(df, output_csv_path)

# Database connection
conn = sqlite3.connect(dbname)
load_to_db(df, conn, tablename)

# Queries
run_query(f"SELECT * FROM {tablename}", conn)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {tablename}", conn)
run_query(f"SELECT Name FROM {tablename} LIMIT 5", conn)

log_progress("Finishing the program and closing the connection.")
conn.close()


