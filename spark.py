from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, concat, explode, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import clickhouse_connect
from dotenv import dotenv_values
import pathlib
import random
import datetime
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script_path}/.env')

def get_candidates():
    try:
        client = clickhouse_connect.get_client(
                        host=config['CLICKHOUSE_HOST'],
                        port=int(config['CLICKHOUSE_PORT']),
                        username=config['CLICKHOUSE_USERNAME'],
                        password=config['CLICKHOUSE_PASSWORD']
                        )
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
    results = client.query('select id from voting_system.candidates')
    candidates_lst = []
    for t in results.result_rows:
        candidates_lst.append(t[0])
    return candidates_lst

# Transform data from Kafka
def transform_data(df):
    json_schema = StructType([
        StructField('results', ArrayType(
            StructType([
                StructField("gender", StringType(), True), #
                StructField("name", StructType([
                    StructField("title", StringType(), True),
                    StructField("first", StringType(), True),
                    StructField("last", StringType(), True)
                ]), True),                                  #
                StructField("location", StructType([
                    StructField("street", StructType([
                        StructField("number", IntegerType(), True),
                        StructField("name", StringType(), True)
                    ]), True),                              #
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("postcode", IntegerType(), True),
                    StructField("coordinates", StructType([
                        StructField("latitude", StringType(), True),  # Dữ liệu latitude là chuỗi
                        StructField("longitude", StringType(), True)  # Dữ liệu longitude là chuỗi
                    ]), True),
                    StructField("timezone", StructType([
                        StructField("offset", StringType(), True),
                        StructField("description", StringType(), True)
                    ]), True)
                ]), True),                                  #
                StructField("email", StringType(), True),   #
                StructField("login", StructType([
                    StructField("uuid", StringType(), True),
                    StructField("username", StringType(), True),
                    StructField("password", StringType(), True),
                    StructField("salt", StringType(), True),
                    StructField("md5", StringType(), True),
                    StructField("sha1", StringType(), True),
                    StructField("sha256", StringType(), True)
                ]), True),                                   #
                StructField("dob", StructType([
                    StructField("date", StringType(), True),  # Có thể thay đổi thành DateType nếu cần
                    StructField("age", IntegerType(), True)
                ]), True),                                  #
                StructField("registered", StructType([
                    StructField("date", StringType(), True),  # Có thể thay đổi thành DateType nếu cần
                    StructField("age", IntegerType(), True)
                ]), True),                                  #
                StructField("phone", StringType(), True),
                StructField("cell", StringType(), True),
                StructField("id", StructType([
                    StructField("name", StringType(), True),
                    StructField("value", StringType(), True)
                ]), True),                                  #
                StructField("picture", StructType([
                    StructField("large", StringType(), True),
                    StructField("medium", StringType(), True),
                    StructField("thumbnail", StringType(), True)
                ]), True),                          #
                StructField("nat", StringType(), True)
            ])                            
        ), True),
        StructField("info", 
            StructType([
                StructField("seed", StringType(), True),
                StructField("results", IntegerType(), True),
                StructField("page", IntegerType(), True),
                StructField("version", StringType(), True)
            ]), True)
    ])
    df = df.withColumn("value", df.value.cast("String")).select("value")
    df = df.withColumn("value", from_json(col('value'), json_schema)).select('value.results')
    df = df.select(explode(df.results).alias('results'))
    df_transformed = df.select(
            col("results.login.uuid").alias("id"),
            col("results.name.first").alias("first_name"),
            col("results.name.last").alias("last_name"),
            col("results.gender").alias("gender"),
            concat(col("results.name.first"), lit(" "), col("results.name.last")).alias("full_name"),
            col("results.email").alias("email"),
            col("results.dob.date").alias("date_of_birth"),
            col("results.dob.age").alias("age"),
            col("results.phone").alias("phone_number"),
            concat_ws(" ", col("results.location.street.name")).alias("street"),    # Allow to add separator when concatenate strings
            col("results.location.city").alias("city"),
            col("results.location.state").alias("state"),
            col("results.nat").alias("national_id"),
            col("results.location.country").alias("country")
        )
    
    return df_transformed

def data_output(df, batch_id):
    driver = 'ru.yandex.clickhouse.ClickHouseDriver'
    username = config["CLICKHOUSE_USERNAME"]
    password = config["CLICKHOUSE_PASSWORD"]
    host = config["CLICKHOUSE_HOST"]
    port = config["CLICKHOUSE_PORT"]
    database = 'voting_system'
    url = f'jdbc:clickhouse://{host}:{port}/{database}'
    (df
    .write
    .mode('append')
    .format('jdbc')
    .option('driver', driver)
    .option('url', url)
    .option('user', username)
    .option('password', password)
    .option('dbtable', 'voting_system.voters')
    .save()
    )
    candidates_lst = get_candidates()
    df_votes = df.select(col('id').alias('voter_id'), lit(random.choice(candidates_lst)).alias('candidate_id'), lit(datetime.datetime.now().replace(microsecond=0)).alias('voting_time'))

    (df_votes
    .write
    .mode('append')
    .format('jdbc')
    .option('driver', driver)
    .option('url', url)
    .option('user', username)
    .option('password', password)
    .option('dbtable', 'voting_system.votes')
    .save())


    print(f"Wrote to ClickHouse successfully!, Batch: {batch_id}")

spark = (SparkSession
         .builder
         .appName('Test')
         .config('master', 'local[*]')
         .config('spark.ui.port', 4050)
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2')
         .getOrCreate()
)

# Schema of voters
voters_schema = StructType(
    [   
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("date_of_birth", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("phone_number", StringType(), False),
        StructField("street", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("country", StringType(), False),
        StructField("national_id", StringType(), False)
    ]
)

# Read data from kafka topic, with stream
kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-1:29092")
    .option("subscribe", "voting_sys_voters")
    .option("startingOffsets", "earliest")
    .load())

df_transformed = transform_data(kafka_df)
df_transformed2 = df_transformed.filter(df_transformed.age > 18)

(
    df_transformed2
    .writeStream
    .foreachBatch(data_output)
    .trigger(processingTime='30 seconds')
    .option('checkpointLocation', 'checkpointLocation/checkpoint_dir_kafka')
    .start()
    .awaitTermination()
)
