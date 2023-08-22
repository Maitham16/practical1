import csv
from kafka import KafkaConsumer
import json

def start_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'  # To start reading from the beginning of the topic
    )

    # Define the columns for the CSV
    columns = [
        "timestamp", "car_id", "model", "current_speed", "battery_capacity", 
        "charge", "consumption", "engine_power", "engine_torque",
        "location", "node", "charging", "distance_covered", "battery_life"
    ]

    # Create a CSV file and write the headers
    with open(f'{topic_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Write headers to CSV
        writer.writerow(columns)

        # Iterate over the messages in the Kafka stream
        for msg in consumer:
            data = msg.value
            print(f"Received data from {topic_name}: {data}")
            
            # Write the data to the CSV file using the order defined in columns
            writer.writerow([data[col] for col in columns])

if __name__ == "__main__":
    start_consumer('node1_data')