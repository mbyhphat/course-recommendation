from kafka import KafkaProducer 
import json
import time 

producer = KafkaProducer(bootstrap_servers=['20.184.50.48:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic_name1 = 'test1'
topic_name2 = 'test2'

print("Waiting for consumer initialization...")
time.sleep(5)

print(f'Start send topic1 data')
with open('registered_courses.txt', 'r', encoding='utf-8') as f:
    try: 
        for i, line in enumerate(f): 
            producer.send(topic_name1, value={"data": line.strip()})
            print(f'sent data {i}: {line.strip()}')
            time.sleep(5)
    except Exception as e: 
        print(f"Error: {e}")

print(f'Sent topic1 data completely')
producer.flush()
print(f'Start send topic2 data')

with open('test_light.txt', 'r', encoding='utf-8') as f:
    try: 
        for i, line in enumerate(f): 
            producer.send(topic_name2, value={"data": line.strip()})
            print(f'sent data {i}: {line.strip()}')
            time.sleep(5)
    except Exception as e: 
        print(f"Error: {e}")

print(f'Sent topic2 data completely')
producer.flush()
producer.close()
