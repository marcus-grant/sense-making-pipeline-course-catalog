# Step 2: DAG Object and its optional datetime dependencies
from datetime import timedelta
from airflow import DAG
# Step 2: Operators Imports
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
# Step 2: Task Functions Imports
import urllib.request
import time, glob, os, json

# Step 3
def catalog():
    def pull(url):
        resp = urllib.request.urlopen(url).read()
        payload = resp.decode('utf-8')
        return payload
    
    def store(data, file):
        file_buf = open(file, 'w+')
        file_buf.write(data)
        file_buf.close()
        print(f'wrote file: {file}')
    