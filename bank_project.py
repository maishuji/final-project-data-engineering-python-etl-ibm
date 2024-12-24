# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
db_table_name = 'Largest_banks'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            try:
                market_cap = float(col[2].contents[0].rstrip())
                if col[1].findAll('a')[1] is not None and '—' not in col[2]:
                    new_row = {
                        "Name": col[1].findAll('a')[1]['title'].rstrip(),
                        "MC_USD_Billion": market_cap}
                    df.loc[len(df)] = new_row
                    #df = pd.concat([df,df1], ignore_index=True)
            except ValueError:
                print('ValueError')
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df_exchange_rate = pd.read_csv(csv_path)
    dict = df_exchange_rate.set_index('Currency').to_dict()['Rate']
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * dict['GBP']).round(2)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * dict['EUR']).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * dict['INR']).round(2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    # Write the data frame to a CSV file
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql_query(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
print(df)
transform(df, 'exchange_rate.csv')
log_progress('Transformation complete. Initiating Csv Load process')
#print(df)
print(f"MC_EUR_Billion[4] -->  {df['MC_EUR_Billion'][4]}")
load_to_csv(df, 'output.csv')
log_progress('Csv Created. Initiating Database Load process')
conn = sqlite3.connect('Banks.db')
load_to_db(df, conn, db_table_name) # tb name is 'Largest_banks'
log_progress('Database Load complete as table. Running queries')

run_query('SELECT * FROM Largest_banks', conn)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
run_query('SELECT Name from Largest_banks LIMIT 5', conn)
log_progress('ETL process complete. Exiting the code')