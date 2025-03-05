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

# Produce to Kafka
def to_kafka():
    from kafka import KafkaProducer 
    import requests 
    import concurrent.futures
    import datetime 
    import time
    producer = KafkaProducer(
        bootstrap_servers = config['KAFKA_BOOTSTRAP_SERVERS'],
        value_serializer = lambda v: v.encode('utf-8')
    )

    dt_end = datetime.datetime.now() + datetime.timedelta(seconds=600)   
    session = requests.Session()    # Help reuse TCP connection, reduce delay and improve performance
    url = "https://randomuser.me/api?nat=us"
    topic = "voting_sys_voters"
    num_threads = 3 
    requests_per_thread = 10 
    # Fetch data and send
    def fetch_and_send():
        try:
            response = session.get(url, timeout=5)  # Dùng session để tối ưu
            if response.status_code == 200:
                data = response.text
                future = producer.send(topic, data)
                future.add_callback(on_send_success).add_errback(on_send_error)
        except Exception as e:
            print(f"Lỗi khi gửi request: {e}")

    def on_send_success(record_metadata):
        """callback function when sending is successful"""
        print(f"Gửi thành công: Topic={record_metadata.topic}, Partition={record_metadata.partition}, Offset={record_metadata.offset}")

    def on_send_error(excp):
        """callback function when sending fails"""
        print(f"Lỗi khi gửi Kafka: {excp}")  
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            if datetime.datetime.now() > dt_end:
                break
            futures = [executor.submit(fetch_and_send) for _ in range(requests_per_thread * num_threads)]
            concurrent.futures.wait(futures)  # Đợi tất cả request xong trước khi tiếp tục
            time.sleep(0.5)  # Nghỉ 0.5 giây trước khi gửi batch tiếp theo

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

