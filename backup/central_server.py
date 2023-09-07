import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import socket
import argparse
import time
import struct
import threading
import logging
import tempfile
import os
from keras.models import save_model

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO)

# Set the number of retries for sending the global model to local nodes
MAX_RETRIES = 30

# Parse command line arguments for number of nodes and rounds
parser = argparse.ArgumentParser()
parser.add_argument('--num_nodes', type=int, default=4)
parser.add_argument('--num_rounds', type=int, default=10)
args = parser.parse_args()

# Initialize a list to store local updates
local_updates = []

# Extract the number of local nodes and the number of communication rounds from parsed arguments
NUM_LOCAL_NODES = args.num_nodes
NUM_ROUNDS = args.num_rounds

local_updates_lock = threading.Lock()
# Initialize a simple neural network model
def initialize_model():
    return keras.Sequential([
        layers.Dense(12, activation='relu', input_shape=(8,)),
        layers.Dense(8, activation='relu'),
        layers.Dense(1)  # Adjust this according to the node's activation function
    ])

# Function to receive large data over a socket connection
def receive_large_data(conn):
    logging.info("Receiving large data over a socket connection")
    data_size = struct.unpack("!I", conn.recv(4))[0]
    chunks = []
    bytes_received = 0
    while bytes_received < data_size:
        chunk = conn.recv(min(data_size - bytes_received, 4096))
        if not chunk:
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_received += len(chunk)
    return b''.join(chunks)

def server_listen_loop(global_model, stop_event):
    """
    Continuously listen for connections from local nodes.
    """
    global local_updates
    global_model_structure = global_model.summary()

    # Set up a socket to listen for connections from local nodes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12345))
        s.listen()

        while not stop_event.is_set():
            try:
                conn, addr = s.accept()
                logging.info(f"Connection accepted from {addr}")
                thread = threading.Thread(target=handle_connection, args=(conn, addr, global_model_structure))
                thread.start()
            except Exception as e:
                logging.error(f"Error accepting connection: {str(e)}")
                
# Function to handle connections from local nodes and receive their model updates
def handle_connection(conn, addr, global_model_structure):
    global local_updates  # Declare the use of the global variable
    
    try:
        logging.info(f"Handling connection from {addr}")
        data = receive_large_data(conn)
        
        # Create a temporary file to save the model data
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file_name = temp_file.name
        with open(temp_file_name, 'wb') as f:
            f.write(data)
        model = keras.models.load_model(temp_file_name)
        
        # Delete the temporary file
        os.remove(temp_file_name)

         # Verify the model structure
        if model.summary() != global_model_structure:
            raise ValueError("Model structure from node doesn't match the global model")

        with local_updates_lock:
            local_updates.append(model.get_weights())

        conn.sendall("READY".encode())
    except Exception as e:
        logging.error(f"Error handling connection from {addr}: {e}")
    finally:
        conn.close()

# Function to receive model updates from local nodes and aggregate them
def receive_local_updates(global_model):
    local_updates = []
    threads = []
    global_model_structure = global_model.summary()

    # Set up a socket to listen for connections from local nodes
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12345))
        s.listen()

        # Continuously accept connections from local nodes 
        while True:  # Infinite loop for accepting connections
                conn, addr = s.accept()
                logging.info(f"Connection accepted from {addr}")
                thread = threading.Thread(target=handle_connection, args=(conn, addr, global_model_structure))
                thread.start()
                threads.append(thread)

                # Clean up threads that have finished
                threads = [t for t in threads if t.is_alive()]

                # Wait for all threads to complete before continuing
                for thread in threads:
                    thread.join()
                    
                return local_updates

# Function to aggregate model updates received from local nodes
def aggregate_updates(local_updates):
    if not local_updates:
        return []

    aggregated = [np.mean([local_weights[i] for local_weights in local_updates], axis=0)
            for i in range(len(local_updates[0]))]
    
    # Debugging print statements
    print("Number of local updates:", len(local_updates))
    for update in local_updates:
        print("Weights in a local update:", len(update))
    print("Weights in aggregated updates:", len(aggregated))
    
    return aggregated

# Function to update the global model with aggregated weights
def update_global_model(global_model, aggregated_weights):
    logging.info("Updating global model with aggregated weights")
    for weight in aggregated_weights:
        print(weight.shape)
    global_model.set_weights(aggregated_weights)
    return global_model

# Function to send large data over a socket connection
def send_large_data(conn, data):
    logging.info("Sending large data over a socket connection")
    data_size = len(data)
    conn.sendall(struct.pack("!I", data_size))
    conn.sendall(data)

# Function to broadcast the updated global model to all local nodes
def send_global_model(global_model):
    """
    Broadcasts the updated global model to all local nodes.
    """
    # Serialize the model to bytes
    with tempfile.NamedTemporaryFile(suffix='.keras', delete=True) as tmp:
        global_model.save(tmp.name)

        with open(tmp.name, 'rb') as f:
            model_data = f.read()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 12346))

        logging.info("Starting to listen on port 12346 for broadcasting the global model...")
        s.listen()
        logging.info("Now listening on port 12346.")

        s.settimeout(600)  # 10 seconds timeout, adjust as needed

        # Track the number of nodes successfully sent the model
        nodes_successful = 0

        while nodes_successful < NUM_LOCAL_NODES:
            try:
                conn, addr = s.accept()
                with conn:
                    for retry in range(MAX_RETRIES):
                        try:
                            send_large_data(conn, model_data)
                            response = conn.recv(5).decode()
                            if response == "ACK":  
                                nodes_successful += 1
                                logging.info(f"Global model sent successfully to node at {addr}")
                                break
                            else:
                                logging.warning(f"Unexpected response from node at {addr}: {response}")
                        except socket.error as e:
                            logging.error(f"Error sending global model to node at {addr}: {str(e)}")
                            if retry < MAX_RETRIES - 1:  # if not the last retry
                                logging.info(f"Retrying... {retry + 1}/{MAX_RETRIES}")
                            time.sleep(10)  # Sleep before retrying
            except socket.timeout:
                logging.error("Socket timed out waiting for a connection from nodes.")

# Main function to coordinate communication rounds and updates
def main():
    # Initialize the global model
    global_model = initialize_model()
    global_model.compile(optimizer='adam', loss='mean_squared_error')  # Compile here

    # Start the server listening loop in a separate thread
    stop_event = threading.Event()
    server_thread = threading.Thread(target=server_listen_loop, args=(global_model, stop_event))
    server_thread.start()

    # Loop through communication rounds
    for round in range(NUM_ROUNDS):
        logging.info(f'Starting communication round {round+1}/{NUM_ROUNDS}')
        
        # Sleep for some time to allow local updates to be collected
        time.sleep(60)
        
        aggregated_updates = aggregate_updates(local_updates)
        global_model.set_weights(aggregated_updates)
        
        logging.info(f'Round {round+1}/{NUM_ROUNDS} - Model received, aggregated, and updated.')
        
        # Sleep for 60 seconds to simulate a pause before broadcasting the updated model
        time.sleep(60)
        
        # Send the updated model to all local nodes
        send_global_model(global_model)
        logging.info(f'Round {round+1}/{NUM_ROUNDS} - Global model broadcasted to nodes.')

    # Stop the server listening thread
    stop_event.set()
    server_thread.join()

if __name__ == "__main__":
    main()