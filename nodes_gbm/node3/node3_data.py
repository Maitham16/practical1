import logging
import os
import numpy as np
import json
import joblib
from kafka import KafkaConsumer
import csv
import socket
import threading
import time
import matplotlib.pyplot as plt
import struct 
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sklearn.ensemble import GradientBoostingClassifier
import tempfile
import tensorflow as tf

# Initialize Kafka producer for sending data to the central server
central_server_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define constants
BATCH_SIZE = 2500
SERVER_HOST = 'localhost'
SERVER_PORT = 12345
SERVER_SEND_PORT = 12346
KAFKA_TOPIC_TO_SERVER = 'node3_server_data'
TIME_INTERVAL = 60
RETRAIN_INTERVAL = 100

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize global variables and locks
total_predictions = 0
correct_predictions = 0
training_batch = []
accumulated_records = []
gbm_model = joblib.load('/home/maith/Desktop/practical1/models/gbm_model_node3.pkl')

# Load scaler for feature normalization
scaler = joblib.load('/home/maith/Desktop/practical1/models/gbm_scaler_node3.pkl')
model_lock = threading.Lock()
prediction_lock = threading.Lock()
accuracy_list = []

# Function to plot prediction accuracy over time
def plot_accuracy():
    global total_predictions
    global correct_predictions
    global accuracy_list

    if total_predictions == 0:
        return

    accuracy = correct_predictions / total_predictions * 100
    accuracy_list.append(accuracy)

    plt.clf()
    plt.plot(accuracy_list, '-o')
    plt.xlabel('Time (updates)')
    plt.ylabel('Prediction Accuracy (%)')
    plt.title('Prediction Accuracy Over Time')
    plt.pause(0.01)

# Function to write data to CSV file
def write_data_to_csv(writer, data, columns):
    try:
        writer.writerow([data[col] for col in columns])
    except Exception as e:
        print(f"Failed to write data to CSV: {e}")

# Function to preprocess incoming data
def process_data(data):
    data['needs_charge'] = 1 if float(data['charge']) <= 50 else 0
    features = [
        float(data["current_speed"]),
        float(data["battery_capacity"]),
        float(data["charge"]),
        float(data["consumption"]),
        float(data["distance_covered"]),
        float(data["battery_life"]),
        float(data["distance_to_charging_point"]),
        float(data["emergency_duration"])
    ]
    # Replace infinity values with NaN
    if np.isinf(features[6]):
        features[6] = np.nan
    # Replace NaN values with mean values
    features = np.nan_to_num(features, nan=np.nanmean(features))
    label = data['needs_charge']
    return features, label

def preprocess_data(data_list):
    features_list, labels_list = [], []
    for data in data_list:
        features, label = process_data(data)
        features_list.append(features)
        labels_list.append(label)
    
    return np.array(features_list), np.array(labels_list)
    
# Function to retrain the model with new batch of data
def retrain_model(preprocessed_data_batch):
    global gbm_model

    # unpack the data batch
    X, y = preprocessed_data_batch
    
    try:
        logging.info("About to start model retraining...")
        gbm_model.fit(X, y)
        logging.info("Model retrained successfully!")
    except Exception as e:
        logging.error(f"Failed to retrain model: {e}")

def periodic_retraining():
    global training_batch
    while True:
        time.sleep(RETRAIN_INTERVAL)
        with model_lock:
            logging.info("Starting periodic retraining...")
            preprocessed_data_batch = load_and_preprocess_data()
            if len(preprocessed_data_batch[0]) > 0:
                retrain_model(preprocessed_data_batch)
                logging.info("Periodic retraining completed.")
            else:
                logging.info("No new data for retraining. Skipping this cycle.")

def preprocess_single_row(data_row):
    """Process a single row and return its features and label."""
    data_row['needs_charge'] = 1 if float(data_row['charge']) <= 50 else 0
    features = [
        float(data_row["current_speed"]),
        float(data_row["battery_capacity"]),
        float(data_row["charge"]),
        float(data_row["consumption"]),
        float(data_row["distance_covered"]),
        float(data_row["battery_life"]),
        float(data_row["distance_to_charging_point"]),
        float(data_row["emergency_duration"])
    ]
    # Replace infinity values with NaN
    if np.isinf(features[6]):
        features[6] = np.nan
    # Replace NaN values with mean values
    features = np.nan_to_num(features, nan=np.nanmean(features))
    label = data_row['needs_charge']
    return features, label

def load_and_preprocess_data():
    """ Load the last 15000 records from the CSV file and preprocess them. """
    data = pd.read_csv('/home/maith/Desktop/practical1/nodes_gbm/node3/node3_data.csv')
    print(data.columns)
    last_15000_records = data.tail(15000)
    
    features_list, labels_list = [], []
    for _, row in last_15000_records.iterrows():
        features, label = preprocess_single_row(row)
        features_list.append(features)
        labels_list.append(label)
    
    X = np.array(features_list)
    y = np.array(labels_list)
    
    return X, y

# Function to predict if charging is needed
def predict_need_charge(model, scaler, features):
    try:
        feature_names = [
            "current_speed", "battery_capacity", "charge", "consumption",
            "distance_covered", "battery_life", "distance_to_charging_point", 
            "emergency_duration"
        ]
        df = pd.DataFrame([features], columns=feature_names)

        features_scaled = scaler.transform(df)
        prediction = model.predict(features_scaled)
        return int(prediction[0])
    except Exception as e:
        print(f"Prediction error: {e}")
        return None
    
# Function to predict and update the model
def predict_and_update(data):
    global total_predictions
    global correct_predictions
    global training_batch
    global gbm_model

    print("Processing received data...")

    columns = [
        "timestamp", "car_id", "model", "current_speed", "battery_capacity",
        "charge", "consumption", "location", "node", "car_status",
        "distance_covered", "battery_life", "distance_to_charging_point",
        "weather", "traffic", "road_gradient", "emergency", "emergency_duration"
    ]

    try:
        write_header = not os.path.exists('node3_data.csv') 
        with open('node3_data.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            if write_header:
                writer.writerow(columns)
            write_data_to_csv(writer, data, columns)
    except Exception as e:
        print(f"Failed to write to CSV: {e}")
        return

    try:
        features, label = process_data(data)
        with model_lock:
            prediction_gbm = predict_need_charge(gbm_model, scaler, features)

        # print prediction
        logging.info(f"Prediction: {prediction_gbm}")

        with prediction_lock:
            total_predictions += 1
            if prediction_gbm == label:
                correct_predictions += 1
                plot_accuracy()
            training_batch.append((features, label))
            if len(training_batch) == BATCH_SIZE:
                retrain_model(training_batch)
                training_batch = []
        # print accuracy
        logging.info(f"Accuracy: {correct_predictions / total_predictions * 100:.2f}%")

    except Exception as e:
        print(f"Error in prediction and update process: {e}")

# Function to send large data over a socket
def send_large_data(sock, data):
    data_size = len(data)
    sock.sendall(struct.pack("!I", data_size))
    sock.sendall(data)

# Function to receive large data from a socket
def receive_large_data(sock):
    data_size = struct.unpack("!I", sock.recv(4))[0]
    chunks = []
    bytes_received = 0
    while bytes_received < data_size:
        chunk = sock.recv(min(data_size - bytes_received, 4096))
        if chunk == b'':
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_received += len(chunk)
    return b''.join(chunks)

# calculate accuracy
node_accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
print(f"Node accuracy: {node_accuracy}")

# Function to exchange model with server
def exchange_model_with_server(local_model):
    MAX_RETRIES = 3
    RETRY_WAIT = 5
    
    logging.info("Starting model exchange with the server.")
    
    # Step 0: Serialize the model
    with tempfile.NamedTemporaryFile(delete=True, suffix='.pkl') as tmp:
        joblib.dump(local_model, tmp.name)
        with open(tmp.name, 'rb') as f:
            serialized_model = f.read()

    for retry in range(MAX_RETRIES):
        try:
            # Calculate node accuracy
            node_accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
            accuracy_str = str(node_accuracy)

            # Step 1: Node sends its model to the server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # Setting a 10-second timeout for socket operations
                logging.info(f"Attempting to connect to the server at {SERVER_HOST}:{SERVER_PORT}")
                s.connect((SERVER_HOST, SERVER_PORT))
                logging.info(f"Successfully connected to the server at {SERVER_HOST}:{SERVER_PORT}")

                logging.info("Sending local model's accuracy to the server...")
                s.sendall(accuracy_str.encode())
                time.sleep(0.5)

                logging.info("Sending local model to the server...")
                send_large_data(s, serialized_model)
                logging.info("Local model sent successfully.")

                # Step 2: Receive a confirmation from the server
                confirmation = s.recv(1024)
                if not confirmation.decode() == "READY":
                    raise Exception(f"Unexpected server confirmation: {confirmation.decode()}")
                logging.info("Received READY confirmation from the server.")

            # Step 3: Connect back to the server to receive the updated global model
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)
                logging.info(f"Attempting to connect to the server at {SERVER_HOST}:{SERVER_SEND_PORT} for receiving the model.")
                s.connect((SERVER_HOST, SERVER_SEND_PORT))
                logging.info("Connected successfully.")

                logging.info("Waiting to receive the global model from the server...")
                data = receive_large_data(s)

                # Send an acknowledgment after successful receipt
                s.sendall("ACK".encode())

                with tempfile.NamedTemporaryFile(delete=True, suffix='.pkl') as tmp_file:
                    tmp_file.write(data)
                    updated_model = joblib.load(tmp_file.name)

                logging.info("Received global model.")

                return updated_model

        except socket.timeout:
            logging.error("Socket operation timed out. Retrying...")
            time.sleep(RETRY_WAIT * (retry + 1))
            continue

        except Exception as e:
            logging.error(f"Failed to connect or exchange data with the server: {e}. Retrying...")
            time.sleep(RETRY_WAIT * (retry + 1))
            continue

    logging.error("Failed to exchange model with server after maximum retries.")
    return None

# Function to print model accuracy
def print_model_accuracy():
    with prediction_lock:
        node_accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
        print(f"Node accuracy: {node_accuracy:.2f}%")


# Function for periodic model exchange with the server
def periodic_model_exchange():
    global gbm_model, correct_predictions, total_predictions, TIME_INTERVAL
    while True:
        time.sleep(TIME_INTERVAL)  
        try:
            print_model_accuracy()  # Before exchanging models
            updated_model = exchange_model_with_server(gbm_model)
            TIME_INTERVAL = 600
            if updated_model is None:
                print("Model is None.")
            else:
                # Sleep for 1 minute before applying the updated model
                time.sleep(5)
                with model_lock:
                    gbm_model = updated_model

                # Reset prediction counters after model exchange
                correct_predictions = 0
                total_predictions = 0
            print_model_accuracy()  # After exchanging models
        except Exception as e:
            print(f"Error during model exchange: {e}")

# Function to send accumulated data to the central server via Kafka
def send_accumulated_data_to_server():
    global accumulated_records
    try:
        if accumulated_records:
            for record in accumulated_records:
                central_server_producer.send(KAFKA_TOPIC_TO_SERVER, value=record)
            central_server_producer.flush()
            logging.info(f"Sent {len(accumulated_records)} records to the central server via Kafka.")
            accumulated_records = []
    except Exception as e:
        logging.error(f"Failed to send data to the central server via Kafka: {e}")

def consume_kafka_messages_and_send_to_server(topic_name):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )

        for _, msg in enumerate(consumer):
            data = msg.value
            print(f"Received from {topic_name}: {data}")
            
            # Append the received data to accumulated_records
            accumulated_records.append(data)

            if len(accumulated_records) >= BATCH_SIZE:
                send_accumulated_data_to_server()

    except Exception as e:
        print(f"Kafka consumption error: {e}")

# Function to send data to the central server
def send_data_to_server(data):
    global accumulated_records
    accumulated_records.append(data)

    if len(accumulated_records) >= BATCH_SIZE:
        send_accumulated_data_to_server()

# Function to consume messages from a Kafka topic
def consume_kafka_messages(topic_name):
    print("Starting Kafka consumer...")
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )

        for _, msg in enumerate(consumer):
            data = msg.value
            print(f"Received from {topic_name}: {data}")
            predict_and_update(data)

    except Exception as e:
        print(f"Kafka consumption error: {e}")

# Main execution
if __name__ == "__main__":
    data_send_thread = threading.Thread(target=consume_kafka_messages_and_send_to_server, args=('node3_data',))
    data_send_thread.start()

    model_thread = threading.Thread(target=periodic_model_exchange)
    model_thread.start()

    retraining_thread = threading.Thread(target=periodic_retraining)
    retraining_thread.start()

    plt.ion()
    consume_kafka_messages('node3_data')
