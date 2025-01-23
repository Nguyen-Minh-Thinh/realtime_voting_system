from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import dotenv_values
import pathlib

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script_path.parent}/.env')

last_2_days = datetime.now() - timedelta(days=2)    # Get the last date
dag = DAG(
    dag_id = 'Stream',
    start_date=last_2_days,
    schedule_interval='@daily',
    catchup=True  # Start executing from start_date to now
)

def to_kafka():
    from kafka import KafkaProducer 
    import requests 
    import datetime 
    producer = KafkaProducer(
        bootstrap_servers = config['KAFKA_BOOTSTRAP_SERVERS'],
        value_serializer = lambda v: v.encode('utf-8')
    )

    dt_end = datetime.datetime.now() + datetime.timedelta(seconds=60)   
    
    while True:
        if datetime.datetime.now() > dt_end:
            break
        url = "https://randomuser.me/api"
        response = requests.get(url + "?nat=us")    # About 90 requests per minute
        response = response.text  # Return String type

        producer.send('voting_sys_voters', response)
        producer.flush()

start_task = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo "Airflow started!"',
    dag = dag
)

create_table = BashOperator(
    task_id = 'create_table',
    bash_command='python /opt/airflow/dags/tasks/create_table.py'
)

insert_candidates = BashOperator(
    task_id = 'insert_candidates',
    bash_command = 'python /opt/airflow/dags/tasks/insert_candidates.py'
)

stream_to_kafka = PythonOperator(
    task_id = 'stream_to_kafka',
    python_callable = to_kafka,
    dag = dag
)

end_task = BashOperator(
    task_id = 'end_task',
    bash_command= 'echo "Airflow ended!"',
    dag = dag

)


start_task >> create_table >> insert_candidates >> stream_to_kafka >> end_task 

