import csv
import json
import socket
import threading
from kafka import KafkaConsumer
import tensorflow as tf
import tempfile
import struct
import numpy as np
from tensorflow.keras import layers
from tensorflow.keras import regularizers
import tensorflow_federated as tff
import logging

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SERVER_HOST = 'localhost'
SERVER_PORT = 12345
SERVER_SEND_PORT = 12346
BROKER_ADDRESS = 'localhost:9092'
topic_name = ['node1_server_data', 'node2_server_data', 'node3_server_data', 'node4_server_data']
NUM_NODES = 4
AGGREGATION_THRESHOLD = 4
filename = "central_server_data.csv"

def check_columns(filename, expected_columns):
    """Check if rows in the CSV file have the expected number of columns."""
    incorrect_lines = []
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for line_num, row in enumerate(reader, 1):
            if len(row) != expected_columns:
                incorrect_lines.append((line_num, len(row)))
    return incorrect_lines

# Check CSV columns
expected_columns = 18
issues = check_columns(filename, expected_columns)
if issues:
    for line_num, col_count in issues:
        print(f"Line #{line_num} (got {col_count} columns instead of {expected_columns})")
else:
    print("All rows have the expected number of columns.")

def save_data_to_csv(data):
    """Save provided data to CSV."""
    columns = [
        "timestamp", "car_id", "model", "current_speed", "battery_capacity",
        "charge", "consumption", "location", "node", "car_status",
        "distance_covered", "battery_life", "distance_to_charging_point",
        "weather", "traffic", "road_gradient", "emergency", "emergency_duration"
    ]

    # Append data to the CSV
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)

def process_data(data):
    """Process data and return feature set and label."""
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

def extract_data_from_csv(filename):
    """Extract feature set and labels from a CSV file."""
    features_list = []
    labels_list = []

    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            features, label = process_data(row)
            features_list.append(features)
            labels_list.append(label)
            
    return features_list, labels_list

# Use the functions:
filename = "your_file_path_here.csv"
expected_columns = 18
issues = check_columns(filename, expected_columns)

if issues:
    for line_num, col_count in issues:
        print(f"Line #{line_num} (got {col_count} columns instead of {expected_columns})")
else:
    features, labels = extract_data_from_csv(filename)
    print(features, labels)

def train_global_model():
    """Train the global model."""
    X, y = load_csv_data("central_server_data.csv", input_features)

    with global_model_lock:
        global_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        global_model.fit(X, y, epochs=100, batch_size=32, verbose=1)
        score = global_model.evaluate(X, y, verbose=1)
        logger.info("Test loss: %s", score[0])
        logger.info("Test accuracy: %s", score[1])


# Function to create a blank model
def create_blank_model(input_features=18):
    """Create and return a blank model."""
    model = tf.keras.models.Sequential([
        layers.Dense(12, activation='relu', kernel_regularizer=regularizers.l2(0.01), input_shape=(input_features,)),  # L2 regularization
        layers.Dense(8, activation='relu', kernel_regularizer=regularizers.l2(0.01)),  # L2 regularization
        layers.Dense(1, activation='sigmoid')
    ])
    return model

# Global Model Initialization
input_features = 18  
global_model = create_blank_model(input_features)
global_model_lock = threading.Lock()
received_models_lock = threading.Lock() 
received_models = []
received_accuracies = []

def load_csv_data(filename, num_features=18):
    """Load data from CSV and return features and labels."""
    data = np.genfromtxt(filename, delimiter=',', skip_header=1)
    
    # Adjust this if the number of features and the column with your labels changes
    X = data[:, :num_features]
    y = data[:, num_features]  # Assuming that the next column after features is the label

    return X, y

# Utility Functions
def send_large_data(sock, data):
    """Send large data over a socket."""
    data_size = len(data)
    sock.sendall(struct.pack('!I', data_size))
    sock.sendall(data)

def receive_large_data(sock):
    """Receive large data over a socket."""
    data_size = struct.unpack('!I', sock.recv(4))[0]
    received_data = b''
    while len(received_data) < data_size:
        more_data = sock.recv(data_size - len(received_data))
        if not more_data:
            raise ValueError("Received less data than expected!")
        received_data += more_data
    return received_data

def aggregate_models_simple_average(models):
    """Aggregate models using a simple average."""
    reference_weights = models[0].get_weights()
    averaged_weights = []

    for i in range(len(reference_weights)):
        layer_weights_list = np.array([model.get_weights()[i] for model in models])
        averaged_weights.append(np.mean(layer_weights_list, axis=0))

    with global_model_lock:
        global_model.set_weights(averaged_weights)
    global_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

def handle_client_connection(client_socket, client_address):
    """Handle the client connection for incoming models."""
    global received_models, received_accuracies

    try:
        # Receiving the accuracy and model
        accuracy = float(client_socket.recv(1024).decode())
        logger.info("Received accuracy: %s from client %s", accuracy, client_address[0])
        data = receive_large_data(client_socket)

        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            tmp_file.write(data)
            local_model = tf.keras.models.load_model(tmp_file.name, compile=False)

            with received_models_lock: 
                received_models.append(local_model)
                received_accuracies.append(accuracy)
                all_models_received = len(received_models) == NUM_NODES

        # Aggregating
        if all_models_received:
            with received_models_lock:
                logger.info("All models received. Aggregating...")
                aggregate_models_simple_average(received_models)
                received_models = []
                received_accuracies = []
                logger.info("Aggregation complete.")

            # Training the aggregated global model
            train_global_model()

            # Send the trained aggregated model
            with tempfile.NamedTemporaryFile(delete=True) as tmp:
                global_model.save(tmp.name, save_format="h5")
            serialized_model = tmp.read()

            logger.info("Sending trained aggregated global model to node")
            send_large_data(client_socket, serialized_model)

        client_socket.send("READY".encode())
        logger.info("Sent READY confirmation to client")

    except Exception as e:
        logger.error(f"Error handling client {client_address[0]}: {e}")

    finally:
        client_socket.close()

def send_global_model_to_node(client_socket, client_address):
    """Send the global model to a node."""
    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        with global_model_lock:  # Locking while accessing global_model
            global_model.save(tmp.name, save_format="h5")
        serialized_model = tmp.read()

        logger.info("Sending global model to node (%s)", client_address[0])
        send_large_data(client_socket, serialized_model)
        
        # Close the connection after sending
        client_socket.close()

def consume_kafka_messages(topic_names):
    """Consume messages from Kafka topics."""
    print("Starting Kafka consumer...")
    try:
        consumer = KafkaConsumer(
            *topic_names,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id="central_server"
        )

        for _, msg in enumerate(consumer):
            data = msg.value
            print(f"Received from {msg.topic}: {data}")
            save_data_to_csv(data)
        
    except Exception as e:
        print(f"Kafka consumption error: {e}")
        
# Main Server Functions
if __name__ == "__main__":

    def kafka_consumer_thread():
            """Start Kafka Consumer."""
            consume_kafka_messages(topic_name)

    def receive_thread():
        """Listen for incoming models from nodes."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((SERVER_HOST, SERVER_PORT))
            s.listen()
            logger.info("Server listening for incoming models on %s:%s", SERVER_HOST, SERVER_PORT)

            while True:
                client_socket, client_address = s.accept()
                logger.info("Accepted connection from %s:%s", client_address[0], client_address[1])
                threading.Thread(target=handle_client_connection, args=(client_socket, client_address)).start()

    def send_thread():
        """Send the aggregated model back to nodes."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as send_socket:
            send_socket.bind((SERVER_HOST, SERVER_SEND_PORT))
            send_socket.listen()
            logger.info("Server ready to send global models on %s:%s", SERVER_HOST, SERVER_SEND_PORT)

            while True:
                client_socket, client_address = send_socket.accept()
                threading.Thread(target=send_global_model_to_node, args=(client_socket, client_address)).start()

    # Start server threads
    threading.Thread(target=kafka_consumer_thread).start()
    threading.Thread(target=receive_thread).start()
    threading.Thread(target=send_thread).start()