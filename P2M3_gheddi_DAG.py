'''
=================================================
Milestone 3

Nama  : Gedi
Batch : FTDS-022-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
=================================================
'''

from airflow.models import DAG
from datetime import timedelta
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine 
from elasticsearch import Elasticsearch

# Function to fetch data from PostgreSQL
def fetchData(): 
    db_engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/db_phase2')  
    df = pd.read_sql("SELECT * FROM credit;",db_engine)
    df.to_csv('/opt/airflow/dags/data_raw.csv', index=False)   

# Function to clean data retrieved from PostgreSQL
def cleanData():
    df_raw = pd.read_csv('/opt/airflow/dags/data_raw.csv')
    df_clean = df_raw.dropna()
    df_clean = df_raw.drop_duplicates()
    df_clean.columns = [col.replace(' ', '_') for col in df_clean.columns]
    df_clean.columns = [col.lower() for col in df_clean.columns]

    df_clean.to_csv('/opt/airflow/dags/data_clean.csv', index=False) 

# Function to load data to ElasticSearch
def postData(): 
    df_clean = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    es = Elasticsearch('http://elasticsearch:9200')

    for i,row in df_clean.iterrows():
        doc=row.to_json()
        es.index(index="m3-airflow", doc_type = 'doc', body=doc)  

default_args= {
    'owner': 'M Gedi',
    'start_date': datetime(2023, 10, 2,11,55,0)- timedelta(hours=7)
} 

with DAG(
    "Milestone_3_Pipeline",
    description='ML scheduling with Airflow', 
    #schedule_interval='*/2 * * * *',
    schedule_interval=None,
    default_args=default_args, 
    catchup=False) as dag:

    fetch_data = PythonOperator(
        task_id='Fetch_SQL_Data',
        python_callable=fetchData
    )

    clean_data = PythonOperator(
        task_id='Clean_Data',
        python_callable=cleanData
    )

    post_to_elasticsearch = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=postData
    )

    fetch_data >> clean_data >> post_to_elasticsearch




