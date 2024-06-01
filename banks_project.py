import pandas as pd
import numpy as np
import requests
import sqlite3
from datetime import datetime 
from bs4 import BeautifulSoup



def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            name = col[1].get_text(strip=True)
            market_cap_str = col[2].get_text(strip=True).replace('\n', '')
            market_cap = float(market_cap_str)
            
            data_dict = {"Name": name, 
                        "MC_USD_Billion": market_cap}
            
            df1 = pd.DataFrame(data_dict, index=[0])
            
            df = pd.concat([df, df1], ignore_index=True)

    return df
   
def transform(df, csv_path):
    read_df = pd.read_csv(csv_path)
    exchange_rate = read_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    for query_statement in query_statements:
        print(query_statement)

        query_output = pd.read_sql(query_statement, sql_connection)
        print(query_output)


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
rate_csv_path = '/home/project/exchange_rate.csv'
output_csv_path = '/home/project/Largest_banks.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df,rate_csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statements = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]

run_query(query_statements, sql_connection)

log_progress('Process Complete.')

sql_connection.close()