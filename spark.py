from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType, col, from_json, lit
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import datetime
import json
import mysql.connector
import requests 
import random
from dotenv import dotenv_values
import pathlib

script = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script}/.env')
random.seed(2025)
# Connect to MySQL
def mysql_connection():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            port='3306',
            user=config['MYSQL_USER'],
            password=config['MYSQL_PASSWORD'],
            database='voting_system')
        
        if conn.is_connected():
            print('Connected to MySQL database')
    except Exception as e:
        print('Error when connecting to MySQL', e)
    else:
        return conn

# Create table in MySQL
def insert_into_candidates(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM candidates')
    rows = cursor.fetchall()
    if len(rows) < 3:
        count = len(rows)
        while count < 3:
            url = "https://randomuser.me/api"
            response = requests.get(url + "?nat=us")
            data = json.loads(response.text)
            data = data['results'][0]
            
            id = data['login']['uuid']
            first_name = data['name']['first']
            last_name = data['name']['last']
            full_name = first_name + ' ' + last_name 
            street = " ".join(map(str, data['location']['street'].values()))
            city = data['location']['city']
            state = data['location']['state']
            country = data['location']['country']
            postcode = data['location']['postcode']
            email = data['email']
            date_of_birth = str(datetime.datetime.strptime(data['dob']['date'], "%Y-%m-%dT%H:%M:%S.%fZ").date())    
            age = data['dob']['age']
            if age < 21:
                continue
            phone_number = data['phone']
            picture_url = data['picture']['large']
            national_id = data['nat']

            query = '''
                    INSERT INTO candidates
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                    '''
            cursor.execute(query, (id, first_name, last_name, full_name, street, city, state, country, postcode, email, date_of_birth, age, phone_number, picture_url, national_id))
            conn.commit()
            count += 1
    cursor.close()

def get_data_from_candidates(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM candidates')
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Initiate SparkSession
spark = (
    SparkSession
    .builder
    .appName("Test1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33")
    .getOrCreate()
)

# Read data from kafka topic, with stream
kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "voting_sys_voters")
    .option("startingOffsets", "earliest")
    .load())

kafka_df_value = kafka_df.withColumn("value", kafka_df.value.cast("String")).select("value")
# Select some specific fields
def select_fields(string_val):
    dict_val = dict()
    data = json.loads(string_val)
    data = data['results'][0]
    id = data['login']['uuid']
    dict_val['id'] = id

    first_name = data['name']['first']
    dict_val['first_name'] = first_name

    last_name = data['name']['last']
    dict_val['last_name'] = last_name

    full_name = first_name + ' ' + last_name 
    dict_val['full_name'] = full_name

    email = data['email']
    dict_val['email'] = email

    date_of_birth = str(datetime.datetime.strptime(data['dob']['date'], "%Y-%m-%dT%H:%M:%S.%fZ").date())
    dict_val['date_of_birth'] = date_of_birth

    age = data['dob']['age']
    dict_val['age'] = age

    phone_number = data['phone']
    dict_val['phone_number'] = phone_number

    street = " ".join(map(str, data['location']['street'].values()))
    dict_val['street'] = street
    
    city = data['location']['city']
    dict_val['city'] = city

    state = data['location']['state']
    dict_val['state'] = state

    country = data['location']['country']
    dict_val['country'] = country
    
    return json.dumps(dict_val)

# Create UDF(User defined function)
select_fields_udf = udf(lambda string_val: select_fields(string_val), StringType())

df_selected_fields = kafka_df_value.select(select_fields_udf(col('value')).alias('value'))

# Schema of voters
voters_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("date_of_birth", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("phone_number", StringType(), False),
        StructField("street", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False)
    ]
)

flattened_voters_df = df_selected_fields.select(from_json(df_selected_fields.value, voters_schema).alias('value'))
df_voters = flattened_voters_df.select("value.*")
df_voters = df_voters.distinct()


def data_output(df, batch_id):
    # print("Batch ID:", batch_id)
    # print("Number of rows:",df.count())
    # Write to MySQL
    (df
    .write
    .mode('append')
    .format('jdbc')
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('url', 'jdbc:mysql://localhost:3306/voting_system')
    .option('dbtable', 'voters')
    .option('user', 'root')
    .option('password', '26122004')
    .save()
    )

    votes_df = df.select(col('id').alias('voter_id')) \
    .withColumn('candidate_id', lit(candidates_data[random.randint(0, 2)][0])) \
    .withColumn('voting_time', lit(datetime.datetime.now().strftime('%Y:%m:%d %H:%M:%S')))
    
    (votes_df
    .write
    .mode('append')
    .format('jdbc')
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('url', 'jdbc:mysql://localhost:3306/voting_system')
    .option('dbtable', 'votes')
    .option('user', 'root')
    .option('password', '26122004')
    .save()
    )

    print(f"Wrote to MySQL successfully!, Batch: {batch_id}")



if __name__ == "__main__":
    mysql_conn = mysql_connection()
    insert_into_candidates(mysql_conn)
    candidates_data = get_data_from_candidates(mysql_conn)
    (
        df_voters
        .writeStream
        .foreachBatch(data_output)
        .trigger(processingTime='10 seconds')
        .option('checkpointLocation', 'checkpoint_dir_kafka')
        .start()
        .awaitTermination()
    )