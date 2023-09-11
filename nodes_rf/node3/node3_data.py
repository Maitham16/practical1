import json
import logging
import numpy as np
from kafka import KafkaConsumer
import csv
import socket
import threading
import time
import matplotlib.pyplot as plt
import struct 
import pandas as pd
import tempfile
import joblib
from sklearn.ensemble import RandomForestClassifier

# Define constants
BATCH_SIZE = 1000
SERVER_HOST = 'localhost'
SERVER_PORT = 12345
SERVER_SEND_PORT = 12346

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize global variables and locks
total_predictions = 0
correct_predictions = 0
training_batch = []

# Load Random Forest model and scaler for feature normalization
rf_model = joblib.load('/home/maith/Desktop/practical1/random_forest_model_node_3.pkl')
logging.info(f"Loaded model type: {type(rf_model)}")
scaler = joblib.load('/home/maith/Desktop/practical1/scaler_node_3.pkl')

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
    if np.isinf(features[6]):
        features[6] = np.nan
    label = data['needs_charge']
    return features, label

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
        return int(prediction)
    except Exception as e:
        print(f"Prediction error: {e}")
        return None

# Function to retrain the model with new batch of data
def retrain_model(batch):
    global rf_model
    X_train = [item[0] for item in batch]
    y_train = [item[1] for item in batch]
    with model_lock:
        rf_model.fit(X_train, y_train)

# Function to predict and update the model
def predict_and_update(data):
    global total_predictions
    global correct_predictions
    global training_batch
    global rf_model

    print("Processing received data...")

    columns = [
        "timestamp", "car_id", "model", "current_speed", "battery_capacity",
        "charge", "consumption", "location", "node", "car_status",
        "distance_covered", "battery_life", "distance_to_charging_point",
        "weather", "traffic", "road_gradient", "emergency", "emergency_duration"
    ]

    try:
        with open('node3_data.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            write_data_to_csv(writer, data, columns)
    except Exception as e:
        print(f"Failed to write to CSV: {e}")
        return

    try:
        features, label = process_data(data)
        with model_lock:
            prediction_nn = predict_need_charge(rf_model, scaler, features)

        print(f"Prediction: {prediction_nn}")

        with prediction_lock:
            total_predictions += 1
            if prediction_nn == label:
                correct_predictions += 1
                plot_accuracy()
            training_batch.append((features, label))
            if len(training_batch) == BATCH_SIZE:
                retrain_model(training_batch)
                training_batch = []

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
    RETRY_WAIT = 5  # Wait time before retrying (this will be increased exponentially)

    if not isinstance(local_model, RandomForestClassifier):
        logging.error("Local model is not a Random Forest classifier.")
        return None
    
    if not hasattr(local_model, 'estimators_'):
        logging.error("Local model hasn't been trained.")
        return None
    
    logging.info("Starting model exchange with the server.")
    
# Step 0: Serialize the model
    with tempfile.NamedTemporaryFile(suffix='.pkl', delete=True) as tmp:
        joblib.dump(local_model, tmp.name)
        serialized_model = tmp.read()

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
                s.sendall(accuracy_str.encode('utf-8'))
                time.sleep(0.5)

                logging.info("Sending local model to the server...")
                send_large_data(s, serialized_model)
                logging.info("Local model sent successfully.")

                # Step 2: Receive a confirmation from the server
                confirmation = s.recv(1024).decode('utf-8')
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

            with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
                tmp_file.write(data)
                updated_model = joblib.load(tmp_file.name)

            # Rest of function unchanged
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
    global rf_model, correct_predictions, total_predictions
    while True:
        time.sleep(60)  # Wait for a specified time interval (1 minute)
        try:
            print_model_accuracy()  # Before exchanging models
            updated_model = exchange_model_with_server(rf_model)
            if updated_model is None:
                print("Model is None.")
            else:
                # Sleep for 1 minute before applying the updated model
                time.sleep(5)
                with model_lock:
                    rf_model = updated_model

                # Reset prediction counters after model exchange
                correct_predictions = 0
                total_predictions = 0
            print_model_accuracy()  # After exchanging models
        except Exception as e:
            print(f"Error during model exchange: {e}")

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
    model_thread = threading.Thread(target=periodic_model_exchange)
    model_thread.start()
    plt.ion()
    consume_kafka_messages('node3_data')