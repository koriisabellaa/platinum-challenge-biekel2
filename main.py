from datetime import datetime
import json
import hashlib

from google.cloud import bigquery
from google.oauth2 import service_account
import requests

url = 'https://api-sekolah-indonesia.herokuapp.com/sekolah?page=1&perPage=10000'
SA_CREDENTIALS_FILE = 'credentials-kelompok-2.json'

def extract ():
    response = requests.get(url)
    return response.json()

def transform(raw_data):
    transformed_data = []
    
    for dataSekolah in raw_data:
            transformed_data.append(
                                    {
                                        'super_key': hashlib.md5(str(dataSekolah).encode()).hexdigest(),
                                        'data_sekolah':  json.dumps(dataSekolah),
                                        'input_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    }
                                    )
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
    raw_data = extract()['dataSekolah']
    transformed_data = transform(raw_data)
    
    table_id = 'kelompok_2_stg.data_sekolah'
    load(transformed_data, table_id)