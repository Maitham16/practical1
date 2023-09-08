import numpy as np
import tensorflow as tf
import json
import joblib
from kafka import KafkaConsumer
import csv
import socket
import os
import shutil
import threading
import time 
import matplotlib.pyplot as plt

# Define constants
BATCH_SIZE = 1000
SERVER_HOST = 'localhost'
SERVER_PORT = 12345

# Initialize global variables and locks
total_predictions = 0
correct_predictions = 0
training_batch = []
nn_model = tf.keras.models.load_model('/home/maith/Desktop/practical1/neural_network_model_node_4.h5')
scaler = joblib.load('/home/maith/Desktop/practical1/scaler_node_4.pkl')
model_lock = threading.Lock()
prediction_lock = threading.Lock()
accuracy_list = []

def plot_accuracy():
    global total_predictions
    global correct_predictions
    global accuracy_list

    if total_predictions == 0:
        return

    accuracy = correct_predictions / total_predictions * 100
    accuracy_list.append(accuracy)

    plt.clf()  # Clear the plot
    plt.plot(accuracy_list, '-o')
    plt.xlabel('Time (updates)')
    plt.ylabel('Prediction Accuracy (%)')
    plt.title('Prediction Accuracy Over Time')
    plt.pause(0.01)  # this will pause the plot update for a while

def write_data_to_csv(writer, data, columns):
    """Save received data to CSV."""
    try:
        writer.writerow([data[col] for col in columns])
    except Exception as e:
        print(f"Failed to write data to CSV: {e}")

def process_data(data):
    """Preprocess incoming data."""
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

def predict_need_charge(model, scaler, features):
    """Predict using the trained model."""
    try:
        features_scaled = scaler.transform(np.array(features).reshape(1, -1))
        prediction = model.predict(features_scaled)
        return int(prediction.round())
    except Exception as e:
        print(f"Prediction error: {e}")
        return None

def retrain_model(batch):
    """Retrain the model with new batch of data."""
    global nn_model
    X_train = [item[0] for item in batch]
    y_train = [item[1] for item in batch]
    with model_lock:
        nn_model.train_on_batch(X_train, y_train)

def predict_and_update(data):
    """Make a prediction and update model if necessary."""
    global total_predictions
    global correct_predictions
    global training_batch
    global nn_model

    print("Processing received data...")

    columns = [
        "timestamp", "car_id", "model", "current_speed", "battery_capacity",
        "charge", "consumption", "location", "node", "car_status",
        "distance_covered", "battery_life", "distance_to_charging_point",
        "weather", "traffic", "road_gradient", "emergency", "emergency_duration"
    ]

    try:
        with open('node4_data.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            write_data_to_csv(writer, data, columns)
    except Exception as e:
        print(f"Failed to write to CSV: {e}")
        return

    try:
        features, label = process_data(data)
        with model_lock:
            prediction_nn = predict_need_charge(nn_model, scaler, features)

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

def exchange_model_with_server(local_model):
    """Exchange model with server."""
    # Save model to a temporary directory
    temp_dir = "temp_model_dir"
    zip_name = "model.zip"
    
    local_model.save(temp_dir)
    shutil.make_archive(zip_name.replace('.zip', ''), 'zip', temp_dir)
    
    # Send the model to the central server
    with open(zip_name, 'rb') as f:
        serialized_model = f.read()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((SERVER_HOST, SERVER_PORT))
        s.sendall(serialized_model)
        data = b''
        while True:
            packet = s.recv(4096)
            if not packet:
                break
            data += packet

    # Receive the updated model from the server and load it
    with open(zip_name, 'wb') as f:
        f.write(data)

    shutil.unpack_archive(zip_name, temp_dir)
    updated_model = tf.keras.models.load_model(temp_dir)
    shutil.rmtree(temp_dir)
    os.remove(zip_name)
    
    return updated_model

def periodic_model_exchange():
    """Periodically exchange the model with the server."""
    global nn_model
    while True:
        time.sleep(600)  # Wait for 10 minutes
        try:
            with model_lock:
                nn_model = exchange_model_with_server(nn_model)
        except Exception as e:
            print(f"Error during model exchange: {e}")

def consume_kafka_messages(topic_name):
    """Consume messages from a Kafka topic."""
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

if __name__ == "__main__":
    model_thread = threading.Thread(target=periodic_model_exchange)
    model_thread.start()
    plt.ion()
    consume_kafka_messages('node4_data')
