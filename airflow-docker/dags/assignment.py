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
    urls = [
        'http://student.mit.edu/catalog/m1a.html',
        'http://student.mit.edu/catalog/m1b.html',
        'http://student.mit.edu/catalog/m1c.html',
        'http://student.mit.edu/catalog/m2a.html',
        'http://student.mit.edu/catalog/m2b.html',
        'http://student.mit.edu/catalog/m2c.html',
        'http://student.mit.edu/catalog/m3a.html',
        'http://student.mit.edu/catalog/m3b.html',
        'http://student.mit.edu/catalog/m4a.html',
        'http://student.mit.edu/catalog/m4b.html',
        'http://student.mit.edu/catalog/m4c.html',
        'http://student.mit.edu/catalog/m4d.html',
        'http://student.mit.edu/catalog/m4e.html',
        'http://student.mit.edu/catalog/m4f.html',
        'http://student.mit.edu/catalog/m4g.html',
    ]
    # urls = []
    # with open('urls.txt', 'r') as f:
    #     for line in f:
    #         url = line.strip()
    #         urls.append(url)
    
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
    with open('data/combo.html', 'w+') as outfile:
        for file in glob.glob('data/*.html'):
            if 'combo.html' in file:
                continue
            with open(file) as infile:
                outfile.write(infile.read())

# Step 6
def titles():
    from bs4 import BeautifulSoup
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print(f'wrote file: {file}')

    with open('data/combo.html', 'r', encoding='utf-8') as file:
        html = file.read()
    html = html.replace('\n', ' ').replace('\r', '')
    soup = BeautifulSoup(html, 'html.parser')
    h3_tags = soup.find_all('h3')

    titles = []
    for tag in h3_tags:
        text = tag.text.strip()
        titles.append(text)
    store_json(titles, 'data/titles.json')

# Step 7
def clean():
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print(f'wrote file: {file}')
    
    with open('data/titles.json', 'r', encoding='utf-8') as file:
        titles = json.load(file)
        for index, title in enumerate(titles):
            punctuation = '''!()-[]{};:'"\,<>./?@#$%^&*_~1234567890'''
            translation_table = str.maketrans('', '', punctuation)
            clean = title.translate(translation_table)
            titles[index] = clean
    
    for index, title in enumerate(titles):
        clean = ' '.join([w for w in title.split() if len(w) > 1])
        titles[index] = clean
    
    store_json(titles, 'data/titles_clean.json')

# Step 8
def count_words():
    from collections import Counter
    def store_json(data, file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print(f'wrote file: {file}')
    
    with open('data/titles_clean.json', 'r', encoding='utf-8') as file:
        titles = json.load(file)

    words = []
    for title in titles:
        words.extend(title.split())
    
    counts = Counter(words)
    store_json(counts, 'data/words.json')

### Airflow Section
# Step 9
with DAG(
    'assignment',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define the tasks in the DAG
    t0 = BashOperator(
        task_id='task_zero',
        bash_command='pip install beautifulsoup4',
    )
    t1 = PythonOperator(
        task_id='task_one',
        python_callable=catalog,
    )
    t2 = PythonOperator(
        task_id='task_two',
        python_callable=combine,
    )
    t3 = PythonOperator(
        task_id='task_three',
        python_callable=titles,
    )
    t4 = PythonOperator(
        task_id='task_four',
        python_callable=clean,
    )
    t5 = PythonOperator(
        task_id='task_five',
        python_callable=count_words,
    )
    # Define the downstream sequences of the DAG
    t0.set_downstream(t1)
    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(t4)
    t4.set_downstream(t5)

# catalog()
# combine()
# titles()
# clean()
# count_words()
