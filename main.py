from datetime import datetime
import json
import hashlib

from google.cloud import bigquery
from google.oauth2 import service_account
import requests

url = 'https://data.covid19.go.id/public/api/prov.json'
SA_CREDENTIALS_FILE = 'credentials-kelompok-2.json'

def extract ():
    response = requests.get(url)
    return response.json()

def transform(raw_data):
    transformed_data = []
    
    for list_data in raw_data:
            transformed_data.append(
                                    {
                                        'super_key': hashlib.md5(str(list_data).encode()).hexdigest(),
                                        'covid_data_province': list_data,
                                        'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    })
    return transformed_data

def load(transformed_data, table_id):
    credential = service_account.Credentials.from_service_account_file(
            SA_CREDENTIALS_FILE,
    )
    
    client = bigquery.Client(
            credentials=credential, 
            project=credential.project_id,
    )
    
    client.insert_rows_json(table_id, transformed_data)
    
    print('Data loaded to BigQuery')


if __name__ == '__main__':
    raw_data = extract()['list_data']
    transformed_data = transform(raw_data)
    
    table_id = 'kelompok_2_stg.covid_data_province'
    load(transformed_data, table_id)