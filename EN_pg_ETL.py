# Code for ETL operations from Engaging Networks into PostgreSQL

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
import time

def timestamp():
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    return now.strftime(timestamp_format) 

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp() + ' : ' + message + '\n')

# Connect to Engaging Networks Bulk API service with HTTPS GET Request to download user data and transactional data

def authenticate(base_url, user_token):
    return requests.post(base_url + "authenticate", data=user_token).json()["ens-auth-token"]

def begin_export(base_url, ens_auth_token):

    payload = {
        "displayUserDataInTransactionExport": True,
        "applyCustomReferenceNames": False,
        "fileType": "csv",
        "queryName": "Data_Administration_Bulk_Export_All_Users",
        "format": "User data",
        "fieldGroup": "apiexport"
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ens-auth-token": ens_auth_token
    }

    response = requests.post(base_url + "exportjob", json=payload, headers=headers)

    return response.json()['id']

def check_progress(base_url, ens_auth_token, job_id):

    headers = {
        'Accept': "application/json",
        'ens-auth-token': ens_auth_token
    }

    response = requests.get (base_url + "exportjob/" + str(job_id), headers=headers)

    return response.json()['status']

def waiting_for_export(base_url, ens_auth_token, job_id):

    while check_progress(base_url, ens_auth_token, job_id) != "completed":
        time.sleep(10)
        log_progress("Checking export status.")
    
def extract(base_url, ens_auth_token, job_id):

    headers = {
        'Accept': "text/csv",
        'ens-auth-token': ens_auth_token
    }

    response = requests.get (base_url + "exportjob/" + str(job_id) + "/download", headers=headers)

    export_file_name = "export_"+ str(job_id) + ".csv"

    export_file_csv = open(export_file_name, "w")

    export_file_csv.write(response.text)

    export_file_csv.close()

    df = pd.read_csv(export_file_name)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def transform(df):


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



    
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

base_url = "https://us.engagingnetworks.app/ens/service/"
user_token = open("user_token.txt", "r").readline().strip()
#table_attribs = [headers]
db_name = 'ccan.db'
table_name = ''
csv_path = "engaging_networks_backup_" + timestamp() + ".csv"

log_progress('Preliminaries complete. Initiating ETL process')

ens_auth_token = authenticate(base_url, user_token)
job_id = begin_export(base_url, ens_auth_token)
print(job_id)

check_progress(base_url, ens_auth_token, job_id)

waiting_for_export(base_url, ens_auth_token, job_id)

log_progress('Data export complete. Initiating extraction process')

df = extract(base_url, ens_auth_token, job_id)

log_progress("File exported from Engaging Networks successfully.")

print(df.head)

'''
df = extract_from_json(response)
log_progress('Data extracted from JSON to Python DataFrame')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to backup CSV file')
sql_connection = create_engine('postgresql://ccan_server:password@ccan:5432/ccan')
log_progress('PostgreSQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
'''