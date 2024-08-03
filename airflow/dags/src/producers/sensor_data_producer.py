"""Module for producing sensor data to Kafka."""

import json
import time
import random
from kafka import KafkaProducer
import yaml

def load_config():
    """Load configuration from YAML file."""
    with open('config/config.yml', 'r') as file:
        return yaml.safe_load(file)

def generate_sensor_data():
    """Generate random sensor data."""
    return {
        "id": random.randint(1, 1000),
        "temperature": round(random.uniform(0, 40), 2),
        "humidity": round(random.uniform(20, 80), 2),
        "timestamp": int(time.time())
    }

class SensorDataProducer:
    """Class for producing sensor data to Kafka."""

    def __init__(self, config):
        """
        Initialize the SensorDataProducer.

        Args:
            config (dict): Configuration dictionary.
        """
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = config['kafka']['topic']

    def produce_data(self):
        """Continuously produce sensor data to Kafka."""
        while True:
            data = generate_sensor_data()
            self.producer.send(self.topic, data)
            print(f"Produced: {data}")
            time.sleep(1)  # Produce data every second

if __name__ == "__main__":
    config = load_config()
    producer = SensorDataProducer(config)
    producer.produce_data()