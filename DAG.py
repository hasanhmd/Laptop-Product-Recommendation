'''
=================================================
Milestone 3

Nama  : Hasan Abdul Hamid
Batch : FTDS-003-SBY

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai adalah dataset mengenai brand laptop di India.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.
def queryPostgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select *, from table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_hasan_abdul_data_raw.csv')
    print("-------Data Saved-------")

# Fungsi ini ditujukan untuk cleaning data
def cleaning():
    # Load data
    df=pd.read_csv('/opt/airflow/dags/P2M3_hasan_abdul_data_raw.csv')

    # Mengganti " " dengan "_" dan semua penamaan dengan huruf kecil
    df.columns = df.columns.str.replace(" ", "_").str.lower()

    # Fungsi menghapus data duplicate
    df = df.drop_duplicates()

    # Fungsi untuk menghapus data storage_type dengan nilai NULL
    df.dropna(subset=['storage_type'], inplace=True)

    # Fungsi untuk mengganti data gpu dengan nilai "Default"
    df['gpu'] = df['gpu'].fillna('Default')

    # Fungsi untuk menghapus data screen dengan nilai NULL
    df.dropna(subset=['screen'], inplace=True)
    
    # Save data clean
    df.to_csv('/opt/airflow/dags/P2M3_hasan_abdul_data_clean.csv')

# Fungsi ini ditujukan untuk upload data clean di Elasticsearch
def insertElasticsearch():
    es = Elasticsearch() 
    df=pd.read_csv('/opt/airflow/dags/P2M3_hasan_abdul_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="frompostgresql",doc_type="doc",body=doc)
        print(res)	

default_args = {
    'owner': 'hasan',
    'start_date': dt.datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      # '5 * * * *',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=queryPostgresql)
    
    cleaning = PythonOperator(task_id='Cleaning',
                                 python_callable=cleaning)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insertElasticsearch)

getData >> cleaning >> insertData
