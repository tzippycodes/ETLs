# Code for ETL operations from Engaging Networks into PostgreSQL

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
import time
import os
import sys as trans
import psycopg2
from sqlalchemy import create_engine

def timestamp():
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    return now.strftime(timestamp_format) 

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp() + ' : ' + message + '\n')

# Check for local CSV to speed ETL testing

def check_for_csv(export_file_name):
    if os.path.isfile(export_file_name):
        return True
    else:

        return False


# Connect to Engaging Networks Bulk API service with HTTPS GET Request to download user data and transactional data

def authenticate(base_url, user_token):
    return requests.post(base_url + "authenticate", data=user_token).json()["ens-auth-token"]

def begin_export(base_url, ens_auth_token, query_name):

    payload = {
        "displayUserDataInTransactionExport": True,
        "applyCustomReferenceNames": False,
        "fileType": "csv",
        "queryName": "query_name",
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

def connect_and_load_to_db(df, table_name):
    ''' This function transforms the final dataframe to as a SQL database table
    with the provided name. Function returns nothing.'''
    engine = create_engine('postgresql://user:password@localhost:5432/database')
    df.to_sql(table_name, engine, if_exists="replace")
    engine.dispose()

    
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

base_url = "https://us.engagingnetworks.app/ens/service/"
user_token = open("user_token.txt", "r").readline().strip()
query_name = "Query_Name"
db_name = 'ccan.db'
table_name = "Engaging_Networks_Bulk_Backup" + timestamp()
csv_path = "engaging_networks_backup_" + timestamp() + ".csv"

if len(trans.argv) > 1 and check_for_csv(trans.argv[1]):
    print("CSV exists")
    df = pd.read_csv(trans.argv[1])
else:
    print("Doesn't exist")

    log_progress('Preliminaries complete. Initiating ETL process')
    ens_auth_token = authenticate(base_url, user_token)
    job_id = begin_export(base_url, ens_auth_token)
    print(job_id)

    check_progress(base_url, ens_auth_token, job_id)

    waiting_for_export(base_url, ens_auth_token, job_id)
    log_progress('Data export complete. Initiating extraction process')

    df = extract(base_url, ens_auth_token, job_id)
    log_progress("File exported from Engaging Networks successfully.")

load_to_csv(df, csv_path)
log_progress('Data saved to backup CSV file')

log_progress('PostgreSQL Connection initiated. Transforming Data to SQL')
connect_and_load_to_db(df, table_name)
log_progress('Data loaded to Database as table. Process Complete.')
