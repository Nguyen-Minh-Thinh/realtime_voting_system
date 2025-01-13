import mysql.connector
from dotenv import dotenv_values
import pathlib

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script_path.parent.parent}/.env')
# print(config)
# print(config['MYSQL_USER'])
def create_table_candidates(conn):
    query = '''
            CREATE TABLE IF NOT EXISTS candidates (
                        id VARCHAR(255) NOT NULL PRIMARY KEY,
                        first_name VARCHAR(50) NOT NULL,
                        last_name VARCHAR(50) NOT NULL,
                        full_name VARCHAR(255),
                        street VARCHAR(255),
                        city VARCHAR(50),
                        state VARCHAR(50),
                        country VARCHAR(50),
                        postcode VARCHAR(20),
                        email VARCHAR(50),
                        date_of_birth DATE,
                        age INT,
                        phone_number VARCHAR(20),
                        picture_url VARCHAR(255),
                        national_id VARCHAR(20)
                    );

            '''
    cursor = conn.cursor()

    cursor.execute(query)
    print('Created table candidates successfully!')
    cursor.close()

def create_table_voters(conn):
    query = '''
            CREATE TABLE IF NOT EXISTS voters (
                        id VARCHAR(255) NOT NULL PRIMARY KEY,
                        first_name VARCHAR(50) NOT NULL,
                        last_name VARCHAR(50) NOT NULL,
                        full_name VARCHAR(255),
                        email VARCHAR(50),
                        date_of_birth DATE,
                        age INT,
                        phone_number VARCHAR(20),
                        street VARCHAR(255),
                        city VARCHAR(50),
                        state VARCHAR(50),
                        country VARCHAR(50)
                    );
            '''
    cursor = conn.cursor()
    cursor.execute(query)
    print('Created table voters successfully!')
    cursor.close()

def create_table_votes(conn):
    query = '''
            CREATE TABLE IF NOT EXISTS votes (
                        voter_id VARCHAR(255),
                        candidate_id VARCHAR(255),
                        voting_time DATETIME,
                        PRIMARY KEY (voter_id , candidate_id),
                        foreign key (voter_id) references voters(id),
                        foreign key (candidate_id) references candidates(id)
                    );

            '''
    cursor = conn.cursor()
    cursor.execute(query)
    print('Created table votes successfully!')
    cursor.close()

if __name__ == '__main__':
    conn = None
    try:
        conn = mysql.connector.connect(host='host.docker.internal',
                                port='3306',
                                database='voting_system',
                                user=config['MYSQL_USER'],
                                password=config['MYSQL_PASSWORD'])
        if conn.is_connected():
            print("Connected MySQL successfully!")
    except Exception as e:
        print("Error when connecting to MySQL!")
    if conn:
        create_table_candidates(conn)
        create_table_voters(conn)
        create_table_votes(conn)
        conn.close()
        print('Connection have been closed!')

