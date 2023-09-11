from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator 
import pandas as pd
import requests 
import json
import boto3
import os

def captura_conta_dados():
        url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
        response = requests.get(url)
        #transformar json em DF
        data = response.json()
        df = pd.DataFrame(data)
        return df

def insert_db(ti):

        df = ti.xcom_pull(task_ids = 'captura_conta_dados')
        
        df.to_csv("dataframe.csv")
        # Configurar as credenciais AWS
        aws_access_key_id = 'AKIATYGP6OBAXBO3OLML'
        aws_secret_access_key = '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'
        s3_bucket_name = 'awsextreme'
        csv_file_name = 'dataframe.csv'

        # Inicializar o cliente S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Upload do arquivo CSV para o Amazon S3
        s3.upload_file(csv_file_name, s3_bucket_name, csv_file_name)

        return 'import_bi'

def import_bi():
        aws_access_key_id = 'AKIATYGP6OBAXBO3OLML'
        aws_secret_access_key = '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'
        s3_bucket_name = 'awsextreme'
        csv_file_name = 'dataframe.csv'

        desktop_path = os.path.expanduser("~/Desktop")

        
        # Crie uma instância do cliente S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Faça o download do arquivo do S3
        s3.download_file(s3_bucket_name, csv_file_name, desktop_path)


with DAG('aprendendo_dag', start_date= datetime(2023,9,11), schedule_interval= "0 0 * * *", catchup= False) as dag:
        captura_conta_dados = PythonOperator (
                task_id = 'captura_conta_dados',
                python_callable= captura_conta_dados
        )

        insert_db = BranchPythonOperator(
                task_id = "insert_db",
                python_callable = insert_db
        )

        import_bi = BranchPythonOperator(
                task_id = "import_bi",
                python_callable = import_bi
        )


        captura_conta_dados >> insert_db >> import_bi