import csv
from kafka import KafkaConsumer
import json

def start_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Create a CSV file and write the headers
    with open(f'{topic_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow(["simulated_timestamp", "car_id", "model", "current_speed", "battery_capacity", 
                         "charge", "consumption", "engine_power", "engine_torque",
                         "location", "node", "charging", "distance_covered"])

        # Iterate over the messages in the Kafka stream
        for msg in consumer:
            data = msg.value
            print(f"Received data from {topic_name}: {data}")
            
            simulated_timestamp = data['timestamp']
            
            # Write the data to the CSV file
            writer.writerow([simulated_timestamp] + [data[key] for key in data])

if __name__ == "__main__":
    start_consumer('node1_data')