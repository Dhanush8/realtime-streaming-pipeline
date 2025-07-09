import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'kasun',
    'start_date': datetime(2025, 7, 5, 4, 00)
}

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60: # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            producer.flush()
        except Exception as e:
            logging.error(f'An Error Occured: {e}')
            continue

with DAG(
    dag_id='user_data_stream',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["kafka"],
) as dag:

    task_stream = PythonOperator(
        task_id='stream_user_data',
        python_callable=stream_data
    )