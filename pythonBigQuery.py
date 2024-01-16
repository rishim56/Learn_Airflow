from google.cloud import bigquery
import os
credential_path = 'C:/Users/003AY5744/Documents/Airflow learn/pythonbq.PrivateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

client = bigquery.Client()
table_id = 'sylvan-plane-408707.dataset_first.dataset_employee'

rows_to_insert = [
    {u'EMP_ID':3, u'EMP_NAME':'Rob', u'EMP_DEPT':'CS', u'EMP_SALARY':1000},
    {u'EMP_ID':4, u'EMP_NAME':'Yen', u'EMP_DEPT':'IT', u'EMP_SALARY':2000}
]

errors = client.insert_rows_json(table_id, rows_to_insert)

if errors == []:
    print('New rows added')
else:
    print(f'error while inserting rows :{errors}')
