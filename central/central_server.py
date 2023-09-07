import socket
import threading
import tensorflow as tf
import tempfile
import struct
import numpy as np
from tensorflow.keras import layers

# Define constants
SERVER_HOST = 'localhost'
SERVER_PORT = 12345
SERVER_SEND_PORT = 12346
NUM_NODES = 4  # Number of nodes you have

def create_blank_model():
    model = tf.keras.models.Sequential([
        layers.Dense(12, activation='relu', input_shape=(8,)),
        layers.Dense(8, activation='relu'),
        layers.Dense(1, activation='sigmoid')
    ])
    return model

# Initialize the global model
global_model = create_blank_model()
global_model_lock = threading.Lock()
received_models = []

# The utility functions from your node script:
def send_large_data(sock, data):
    data_size = len(data)
    sock.sendall(struct.pack('!I', data_size))
    sock.sendall(data)

# Function to receive large data from a socket
def receive_large_data(sock):
    data_size = struct.unpack('!I', sock.recv(4))[0]
    received_data = b''
    while len(received_data) < data_size:
        more_data = sock.recv(data_size - len(received_data))
        if not more_data:
            raise ValueError("Received less data than expected!")
        received_data += more_data
    return received_data

# Function to aggregate the local models into the global model
def aggregate_models(models, accuracies):
    global global_model
    
    # Check if the length of models matches the length of accuracies
    if len(models) != len(accuracies):
        raise ValueError("The number of models does not match the number of accuracies.")

    # Convert accuracies into weights. This can be a simple normalization.
    total_accuracy = sum(accuracies)
    weights = [acc/total_accuracy for acc in accuracies]

    # Get weights of the first model as a reference
    reference_weights = models[0].get_weights()

    # Create a list to store averaged weights
    averaged_weights = []

    for i in range(len(reference_weights)):
        # For each set of weights (or biases), get the corresponding weights from each model
        layer_weights_list = [model.get_weights()[i] for model in models]
        
        # Calculate the weighted average of these weights
        weighted_avg_layer_weights = np.average(layer_weights_list, axis=0, weights=weights)
        
        # Append to the averaged_weights list
        averaged_weights.append(weighted_avg_layer_weights)

    with global_model_lock:
        global_model.set_weights(averaged_weights)
        
    # Compile the global model (optional if you want to evaluate or further train it on the central server)
    global_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

# Connection handler for each node
def handle_client_connection(client_socket):
    global received_models

    # Step 1: Receive local model from the node
    data = receive_large_data(client_socket)
    with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
        tmp_file.write(data)
        local_model = tf.keras.models.load_model(tmp_file.name, compile=False)
        global_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    received_models.append(local_model)

    # If we've received models from all nodes, aggregate them
    if len(received_models) == NUM_NODES:
        aggregate_models(received_models)
        received_models = []  # Reset for next round

        # Now send the updated global model back to all nodes
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            global_model.save(tmp.name, save_format="h5")
            serialized_model = tmp.read()
            
            # Send the serialized global model to the node
            send_large_data(client_socket, serialized_model)

    # Send a READY confirmation back to the node
    try:
        client_socket.send("READY".encode())
    except ConnectionResetError:
        print("Connection was reset by client.")
    finally:
        client_socket.close()

def send_global_model_to_node(client_socket):
    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        global_model.save(tmp.name, save_format="h5")
        serialized_model = tmp.read()
        send_large_data(client_socket, serialized_model)
    client_socket.close()

# Main function to start the server...
if __name__ == "__main__":
    # This thread will listen for incoming models from nodes.
    def receive_thread():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((SERVER_HOST, SERVER_PORT))
            s.listen()
            print(f"Server listening for incoming models on {SERVER_HOST}:{SERVER_PORT}")

            while True:
                client_socket, client_address = s.accept()
                threading.Thread(target=handle_client_connection, args=(client_socket,)).start()

    # This thread will send the aggregated model back to nodes.
    def send_thread():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as send_socket:
            send_socket.bind((SERVER_HOST, SERVER_SEND_PORT))
            send_socket.listen()
            print(f"Server ready to send global models on {SERVER_HOST}:{SERVER_SEND_PORT}")

            while True:
                client_socket, client_address = send_socket.accept()
                threading.Thread(target=send_global_model_to_node, args=(client_socket,)).start()

    # Start both threads.
    threading.Thread(target=receive_thread).start()
    threading.Thread(target=send_thread).start()