# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd 
import numpy as np 
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    for row in rows:
        cols = row.find_all('td')
        if len(cols)!=0:
            a_tags = cols[1].find_all('a')
            n = cols[2].contents[0]
            n1 = float(n[:-1])
            data_dict = {
                "Name": a_tags[1].contents[0],
                "MC_USD_Billion": n1
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df 

  

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df 

def load_to_csv(dataframe, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    dataframe.to_csv(output_path)

def load_to_db(dataframe, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)



def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''



log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
dataframe = transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(dataframe, output_path)
log_progress("Data saved to CSV file")
sql_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")
load_to_db(dataframe, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")
query_statement1 = f"SELECT * FROM {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement3 = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)
log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")


print(dataframe)
print(dataframe['MC_EUR_Billion'][4])


