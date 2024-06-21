from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'artist',
    'start_date': datetime(2024,6,13,00)
}

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res


def format_data(res):
    data = {}

    location = res['location']
    data['first_name']= res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['dob'] = res['dob']['date']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['password'] = res['login']['password']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data



def stream_data():
    import json
    import kafka
    import time
    import logging

    
    producer = kafka.KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()

    while True:
        if(time.time() > curr_time+60):
            break
        try:
            _data = get_data()
            data = format_data(_data)
            producer.send('create_user',json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f"Some error occured : {e}")
            continue
    


with DAG ('user_automation',
            default_args=default_args,
            schedule_interval='@daily',
            start_date=datetime(2021, 1, 1),
            catchup=False):


            streaming_task = PythonOperator(
                task_id = 'stream_data_fromm_kafka',
                python_callable=stream_data
            )

