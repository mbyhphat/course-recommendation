from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import time 

# method 1: using spark structured streaming 
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0',
    'org.apache.kafka:kafka-clients:3.9.1'
]

spark = SparkSession.builder \
    .appName('kafka-consumer') \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

kafka_server = '20.184.50.48:9092'
topic_name1 = 'test1'
topic_name2 = 'test2'

# Vì mỗi message là {"data": "text dòng"}
schema = StructType().add("data", StringType())

# Đọc stream từ Kafka
stream_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", f"{topic_name1},{topic_name2}") \
    .option("startingOffsets", "latest") \
    .load()

stream_DF = stream_raw.select(from_json(col("value").cast("string"), schema).alias("parsed"), col("topic")) \
                        .select("parsed.data", "topic").filter(col("data").isNotNull())

stream_DF_topic1 = stream_DF.filter(col("topic") == topic_name1)
stream_DF_topic2 = stream_DF.filter(col("topic") == topic_name2)

stream_writer1 = (stream_DF_topic1.writeStream.queryName('queryData1').trigger(processingTime='3 seconds').outputMode('append').format('memory'))
stream_writer2 = (stream_DF_topic2.writeStream.queryName('queryData2').trigger(processingTime='3 seconds').outputMode('append').format('memory'))

print('Start receive data from topic1')
query1 = stream_writer1.start()
query2 = stream_writer2.start()

max_wait_rounds = 5  # số lần liên tiếp không thấy dữ liệu mới thì dừng
unchanged_rounds = 0
last_count = 0

try: 
    while True: 
        result1 = spark.sql(f'SELECT * FROM queryData1')
        current_count = result1.dropDuplicates().count()
        result1.show()
        print(f"Received data count from topic1: {current_count}")

        if current_count == last_count:
            unchanged_rounds += 1
        else:
            unchanged_rounds = 0  # reset nếu có dữ liệu mới

        if unchanged_rounds >= max_wait_rounds:
            print("[INFO] No new data received in recent rounds. Stopping...")
            break

        last_count = current_count
        time.sleep(5)
except Exception as e: 
    print(f"[ERROR] An exception occurred during streaming query1: {e}")
finally: 
    query1.stop()
    print("[INFO] Stopped streaming query1.")

print('Received data from topic 1 completely')
print('Start receive data from topic2')

max_wait_rounds = 2  # số lần liên tiếp không thấy dữ liệu mới thì dừng
unchanged_rounds = 0
last_count = 0

try: 
    while True: 
        result2 = spark.sql(f'SELECT * FROM queryData2')
        current_count = result2.dropDuplicates().count()
        result2.show()
        print(f"Received data count from topic2: {current_count}")
        if current_count == last_count:
            unchanged_rounds += 1
        else:
            unchanged_rounds = 0  # reset nếu có dữ liệu mới

        if unchanged_rounds >= max_wait_rounds:
            print("[INFO] No new data received in recent rounds. Stopping...")
            break

        last_count = current_count
        time.sleep(5)
except Exception as e: 
    print(f"[ERROR] An exception occurred during streaming: {e}")
finally: 
    query2.stop()
    print("[INFO] Stopped streaming query2.")

print('Received data from topic 2 completely')
    
# result.toPandas().to_csv('received_data.csv', index=False)


# method 2: using KafkaConsumer from kafka 
# consumer = KafkaConsumer('test',
#                          bootstrap_servers=['20.184.50.48:9092'],
#                          value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# data = []
# for c in consumer:
#   print(c.value['data'])
#   data.append(c.value['data'])

# df = pd.DataFrame(data)
# print(df)
