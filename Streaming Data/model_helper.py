import torch
import csv
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_user_dict_from_spark_df(spark_df, n_entities=16118):
    """
    Chuyển đổi PySpark DataFrame thành user dictionary với ánh xạ user_id
    DataFrame cần có cột: user_id, item_id
    user_id gốc sẽ được ánh xạ thành: user_id + n_entities (mapped_user_id)
    """
    
    # Tạo dictionary: {mapped_user_id: [list_of_items]}
    user_dict = dict()
    for row in spark_df.rdd.collect():
        tmp = row[0].strip()
        inter = [int(i) for i in tmp.split()]
        orig_user_id, item_ids = inter[0], inter[1:]

        mapped_user_id = orig_user_id + n_entities  # Ánh xạ như trong code gốc
        item_ids = list(set(item_ids))
        
        user_dict[mapped_user_id] = item_ids
    
    return user_dict

def load_model(model, model_path):
    checkpoint = torch.load(model_path, map_location=torch.device('cpu'))
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    return model

def create_result_file(df, test_user_dict, csv_filename):
    rows = df.select("user_id", "original_user_id", "recommendations").collect()
    
    file_exists = os.path.exists(csv_filename)
    
    with open(csv_filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Chỉ ghi header lần đầu tiên
        if not file_exists:
            writer.writerow(["original_user_id", "gt_index", "top10_preds"])
        
        for row in rows:
            mapping_user_id = row["user_id"]
            original_id = row["original_user_id"]
            recs = row["recommendations"]
            top10 = recs[:10]
            gt_items = test_user_dict.get(mapping_user_id, [])
            
            for gt in gt_items:
                try:
                    gt_index = recs.index(gt)
                except ValueError:
                    gt_index = -1
                writer.writerow([original_id, gt_index, top10])

def create_mapping(result_df, mapping_df, mapping_course, user_df, course_df):
    # Bước 1: Join result_df với mapping_df để lấy mapped_id từ original_user_id
    merged_df = result_df.join(mapping_df, result_df.original_user_id == mapping_df.mapped_id, how="left")

    # Bước 2: Join kết quả với df3 để lấy user_name từ original_id
    final_df = merged_df.join(user_df, merged_df.original_id == user_df.id, how="left")
    final_df = final_df.select("original_user_id", "name", "top10_preds")

    schema = ArrayType(IntegerType())
    final_df = final_df.withColumn("top10_preds", from_json(final_df["top10_preds"],schema))

    df_with_position = final_df.select(
        col("original_user_id"),
        col("name").alias("User Name"),
        posexplode(col("top10_preds")).alias("position", "mapping_id")
    )

    # Join và sắp xếp lại theo thứ tự gốc
    df_ordered = df_with_position.join(mapping_course, df_with_position.mapping_id == mapping_course.mapped_id, how="left") \
        .join(course_df, mapping_course.original_id == course_df.id, how="left") \
        .orderBy("original_user_id", "position")

    # Collect lại với thứ tự
    ordered_result = df_ordered.groupBy("original_user_id", "User Name").agg(
        collect_list("name").alias("Course Name")
    )

    return ordered_result