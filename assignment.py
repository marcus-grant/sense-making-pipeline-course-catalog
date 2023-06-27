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
    
    # Step 4
    urls = []
    with open('urls.txt', 'r') as f:
        for line in f:
            url = line.strip()
            urls.append(url)
    
    for url in urls:
        index = url.rfind('/') + 1
        filename = url[index:]
        file = './data/' + filename
        if not os.path.exists('./data'):
            os.makedirs('./data')
        if os.path.exists(file):
            print(f'{file} already exists')
            print('--- skipping ---')
        else:
            data = pull(url)
            store(data, file)
            print(f'pulled: {file}')
            print('--- waiting ---')
            time.sleep(15)

# Step 5
def combine():
    if not os.path.exists('./data'):
        os.makedirs('./data')
    with open('data/combo.txt', 'w+') as outfile:
        for file in glob.glob('data/*.html'):
            with open(file) as infile:
                outfile.write(infile.read())

combine()
