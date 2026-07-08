from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaProducer
import time

import json
import requests
import uuid

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 1, 1, 00, 00)
}

def get_data():

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    return response


def format_data(response):
    
    data = {}
    location = response['location']

    data['id'] = str(uuid.uuid4())
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while time.time() < curr_time + 60:
        try: 

            response = get_data()
            response = format_data(response)
            producer.send('users_created', json.dumps(response). encode('utf-8'))

        except Exception as e:
            print(e)
            continue

    


with DAG('user_api_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    task = PythonOperator(task_id = 'stream_user_data_from_api', python_callable=stream_data)
