# import mysql.connector
import clickhouse_connect
from dotenv import dotenv_values
import pathlib

def create_table_candidates(client):
    command = f'''
            CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DATABASE']}.candidates
                    (
                        id String NOT NULL, -- String thay vì VARCHAR
                        first_name String NOT NULL,
                        last_name String NOT NULL,
                        full_name String,
                        street String,
                        city String,
                        state String,
                        country String,
                        postcode String,     -- 
                        email String,
                        date_of_birth String, -- Date32 has range year 1925 -> 2283 but it's not incompatible
                        age Int32,          -- Sử dụng kiểu Int32 thay vì INT
                        phone_number String,
                        picture_url String, -- 
                        national_id String  --
                    )
                    ENGINE = MergeTree()
                    PRIMARY KEY id
                    ORDER BY id; -- Yêu cầu bắt buộc với MergeTree

            '''
    client.command(command)
    print('Created table candidates successfully!')

def create_table_voters(client):
    command = f'''
            CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DATABASE']}.voters (
                id String NOT NULL,           
                first_name String NOT NULL,   
                last_name String NOT NULL,    
                full_name String,             
                email String,                 
                date_of_birth String,           
                age Int32,                    
                phone_number String,          
                street String,                
                city String,                  
                state String,                 
                country String                
            )
            ENGINE = MergeTree() 
            PRIMARY KEY id
            ORDER BY id;                     -- Cần có ORDER BY khi sử dụng MergeTree

            '''
    client.command(command)
    print('Created table voters successfully!')

def create_table_votes(client):
    command = f'''
            CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DATABASE']}.votes (
                    voter_id String,
                    candidate_id String,
                    voting_time DateTime,
                    PRIMARY KEY (voter_id, candidate_id)
                ) ENGINE = MergeTree()
                ORDER BY (voter_id, candidate_id);
            '''
    client.command(command)
    print('Created table votes successfully!')

# Pass config dictionary into this function
def get_clickhouse_client(config):
    try:
        return clickhouse_connect.get_client(
                        host=config['CLICKHOUSE_HOST'],
                        port=int(config['CLICKHOUSE_PORT']),
                        username=config['CLICKHOUSE_USERNAME'],
                        password=config['CLICKHOUSE_PASSWORD']
                        )
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
        raise
if __name__ == '__main__':
    script_path = pathlib.Path(__file__).parent.resolve()
    config = dotenv_values(f'{script_path.parent.parent}/.env')
    client = get_clickhouse_client(config)

    client.command('CREATE DATABASE IF NOT EXISTS voting_system')
    create_table_candidates(client)
    create_table_voters(client)
    create_table_votes(client)
    # result = client.query('select * from audio_device_data_pipeline.headphones')
    # print(result.result_rows)
    

    