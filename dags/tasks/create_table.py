# import mysql.connector
import clickhouse_connect
from dotenv import dotenv_values
import pathlib

# Create table candidates
def create_table_candidates(client):
    command = f'''
            CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DATABASE']}.candidates
                    (
                        id String NOT NULL, -- String type in ClickHouse instead of using VARCHAR
                        first_name String NOT NULL,
                        last_name String NOT NULL,
                        full_name String,
                        date_of_birth String, 
                        gender String,
                        age Int32,          -- Int32 type in ClickHouse instead of using INT
                        email String,
                        phone_number String,
                        street String,
                        city String,
                        state String,
                        country String,
                        national_id String,
                        picture_url String
                    )
                    ENGINE = MergeTree()
                    PRIMARY KEY id
                    ORDER BY id; 

            '''
    client.command(command)
    print('Created table candidates successfully!')

# Create table voters
def create_table_voters(client):
    command = f'''
            CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DATABASE']}.voters (
                id String NOT NULL, 
                first_name String NOT NULL,
                last_name String NOT NULL,
                full_name String,
                date_of_birth String, 
                gender String,
                age Int32,          
                email String,
                phone_number String,
                street String,
                city String,
                state String,
                country String,
                national_id String          
            )
            ENGINE = MergeTree() 
            PRIMARY KEY id
            ORDER BY id;                   -- Must have Order by when using MergeTree
            '''
    client.command(command)
    print('Created table voters successfully!')

# Create table votes
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

    

    