from kafka import KafkaProducer 
import json
import time 

producer = KafkaProducer(bootstrap_servers=['20.184.50.48:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic_name = 'test1'

def get_batch_id():
    """Tạo batch ID duy nhất mỗi lần chạy"""
    return int(time.time())

print("Waiting for consumer initialization...")
time.sleep(5)
print(f'Start sending data')

batch_id = get_batch_id()
print(f"Starting batch: {batch_id}")

with open('test_light1.txt', 'r', encoding='utf-8') as f:
    try:
        for i, line in enumerate(f):
            message_with_id = {
                "batch_id": batch_id,    # ID duy nhất cho mỗi lần chạy
                "sequence": i + 1,       # Thứ tự trong batch
                "data": line.strip()
            }
            producer.send(topic_name, value=message_with_id)
            print(f'sent batch {batch_id}, seq {i + 1}: {line.strip()}')
            time.sleep(5)
    except Exception as e:
        print(f"Error: {e}")

print(f'Sent data completely')
producer.flush()
producer.close()
