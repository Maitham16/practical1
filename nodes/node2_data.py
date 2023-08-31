import csv
from kafka import KafkaConsumer
import json

def create_csv_writer(filename, columns):
    try:
        file = open(filename, 'w', newline='')
        writer = csv.writer(file)
        writer.writerow(columns)
        return file, writer
    except Exception as e:
        print(f"Error creating CSV writer: {e}")
        raise

def write_data_to_csv(writer, data, columns):
    try:
        writer.writerow([data[col] for col in columns])
    except Exception as e:
        print(f"Error writing data to CSV: {e}")
        raise

def start_consumer(topic_name, csv_filename):
    file = None
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )

        columns = [
            "timestamp", "car_id", "model", "current_speed", "battery_capacity",
            "charge", "consumption", "location", "node", "car_status",
            "distance_covered", "battery_life", "distance_to_charging_point",
            "weather", "traffic", "road_gradient", "emergency", "emergency_duration"
        ]

        file, writer = create_csv_writer(csv_filename, columns)

        try:
            for msg in consumer:
                data = msg.value
                print(f"Received data from {topic_name}: {data}")
                write_data_to_csv(writer, data, columns)
                file.flush()
        except KeyboardInterrupt:
            pass
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if file:
            file.close()

if __name__ == "__main__":
    start_consumer('node1_data', 'node2_data.csv')