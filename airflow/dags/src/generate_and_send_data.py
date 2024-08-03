from kafka import KafkaProducer
import json
import random
import time

def generate_random_data():
    return {
        "id": random.randint(1, 1000),
        "temperature": round(random.uniform(0, 40), 2),
        "humidity": round(random.uniform(20, 80), 2),
        "timestamp": int(time.time())
    }

def send_data_to_kafka(bootstrap_servers, topic):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        data = generate_random_data()
        producer.send(topic, data)
        print(f"Sent: {data}")
        time.sleep(1)

if __name__ == "__main__":
    bootstrap_servers = ['broker:29092']
    topic = 'sensor_data'
    send_data_to_kafka(bootstrap_servers, topic)