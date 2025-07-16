import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator
import time 
from model_helper import load_model, create_user_dict_from_spark_df, create_result_file, create_mapping
from model.KGAT import KGAT
import pandas as pd
import torch 

@pandas_udf(returnType=ArrayType(IntegerType()))
def predict_recommendations_udf(user_batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF để dự đoán recommendation cho từng batch user
    Input: mapped_user_id (user_id + n_entities)
    Output: top-K item recommendations
    """
    # Khởi tạo model (chỉ làm một lần cho mỗi executor)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    n_users = 70000
    n_entities = 16118
    n_relations = 10
    model = KGAT(
        n_users=n_users, n_entities=n_entities, n_relations=n_relations, use_pretrain=2
    )
    pretrain_model_path = "model_epoch250_new.pth"
    model = load_model(model, pretrain_model_path)
    model.to(device)
    model.eval()
    
    n_items = 3148
    item_ids = torch.arange(n_items, dtype=torch.long).to(device)
    
    for user_batch in user_batch_iter:
        # user_batch chứa mapped_user_id
        batch_user_ids = torch.LongTensor(user_batch.tolist()).to(device)
        
        with torch.no_grad():
            batch_scores = model(batch_user_ids, item_ids, mode="predict")
        
        # Chuyển về numpy
        batch_scores_np = batch_scores.cpu().numpy()
        
        # Lấy top-K items cho từng user
        TOP_K = 100
        recommendations = []
        
        for i in range(len(user_batch)):
            user_scores = batch_scores_np[i]
            top_items = user_scores.argsort()[::-1][:TOP_K]  # indices = item_id gốc
            recommendations.append(top_items.tolist())
        
        yield pd.Series(recommendations)

def predict(train_df, test_df, save_file):
    train_user_dict = create_user_dict_from_spark_df(train_df)
    test_user_dict = create_user_dict_from_spark_df(test_df)

    # Broadcast các dictionary
    bc_train_user_dict = spark.sparkContext.broadcast(train_user_dict)
    bc_test_user_dict = spark.sparkContext.broadcast(test_user_dict)
        
    users_to_predict_df = spark.createDataFrame(
        [(user_id,) for user_id in test_user_dict.keys()], 
        ["user_id"]
    )

    # Áp dụng UDF
    result_df = users_to_predict_df.select(
        col("user_id"),
        predict_recommendations_udf(col("user_id")).alias("recommendations")
    )

    adjusted_df = result_df.withColumn("original_user_id", col("user_id") - 16118)
    adjusted_df.show()

    # Save file from result_df
    create_result_file(adjusted_df, test_user_dict, save_file)


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
topic_name = 'test1'

schema = StructType() \
    .add("batch_id", LongType()) \
    .add("sequence", IntegerType()) \
    .add("data", StringType())


# Đọc stream từ Kafka
stream_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", f"{topic_name}") \
    .option("startingOffsets", "latest") \
    .load()

stream_DF = stream_raw.select(from_json(col("value").cast("string"), schema).alias("parsed")) \
                        .select("parsed.batch_id", "parsed.sequence", "parsed.data").filter(col("data").isNotNull())

stream_writer = (stream_DF.writeStream.queryName('queryData').trigger(processingTime='3 seconds').outputMode('append').format('memory'))
train_df = spark.read.options(header=True, inferSchema=True).text('registered_courses.txt')

# Read user, course, user_mapping, course_mapping file 
mapping_df = spark.read.option("header", "true").csv("user_mapping.csv")
mapping_course = spark.read.option("header", "true").csv("course_mapping.csv")
user_df = spark.read.json("user.json")
course_df = spark.read.json("course.json")

print('Start receiving data')
query = stream_writer.start()

# Track theo batch_id và sequence
processed_batches = {}  # {batch_id: max_sequence_processed}
results_file = "evaluation_results2.csv"

try:
    while True:
        result1 = spark.sql(f'SELECT * FROM queryData')
        current_count = result1.count()
        
        if current_count > 0:
            # Group theo batch_id
            batches = result1.groupBy('batch_id').agg(
                max('sequence').alias('max_seq'),
                count('*').alias('count')
            ).collect()
            
            new_data_found = False
            for batch_row in batches:
                batch_id = batch_row.batch_id
                max_seq = batch_row.max_seq
                
                last_processed_seq = processed_batches.get(batch_id, 0)
                
                if max_seq > last_processed_seq:
                    print(f"New data in batch {batch_id}: seq {last_processed_seq + 1} to {max_seq}")
                    
                    # Lấy data mới từ batch này
                    new_data = result1.filter(
                        (col('batch_id') == batch_id) & 
                        (col('sequence') > last_processed_seq)
                    ).select('data')
                    print('--------Testing data--------')
                    new_data.show()
                    print('--------Start predicting-----')
                    predict(train_df, new_data, results_file)

                    result_df = spark.read.option("header", "true").csv(results_file)
                    ordered_result = create_mapping(result_df, mapping_df, mapping_course, user_df, course_df)

                    print('--------Mapping result----------')
                    ordered_result.show()
                    
                    processed_batches[batch_id] = max_seq
                    new_data_found = True
            
            if not new_data_found:
                print("No new data found")
                
        time.sleep(3)
        
except Exception as e:
    print(f"[ERROR] An exception occurred: {e}")
