import csv
from kafka import KafkaConsumer
import json
from datetime import datetime

def start_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Create a CSV file and write the headers
    with open(f'{topic_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Include timestamp in the header
        writer.writerow(["timestamp", "car_id", "model", "current_speed", "battery_capacity", 
                         "charge", "consumption", "engine_power", "engine_torque",
                         "location", "node", "charging", "distance_covered"])

        # Iterate over the messages in the Kafka stream
        for msg in consumer:
            data = msg.value
            print(f"Received data from {topic_name}: {data}")
            
            # Capture the current timestamp when data is received
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Write the timestamp followed by the other data fields to the CSV file
            writer.writerow([timestamp] + [data[key] for key in data])

if __name__ == "__main__":
    start_consumer('node4_data')