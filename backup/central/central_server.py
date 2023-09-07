import numpy as np
import torch
import torch.nn as nn
import socket
import pickle
import argparse
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Central server for federated learning.')
parser.add_argument('--num_nodes', type=int, default=2, help='Number of local nodes')
parser.add_argument('--num_rounds', type=int, default=10, help='Number of communication rounds')
args = parser.parse_args()

NUM_LOCAL_NODES = args.num_nodes
NUM_ROUNDS = args.num_rounds

# Initialize global model
def initialize_model():
    logging.info('Initializing global model')
    class Net(nn.Module):
        def __init__(self):
            super(Net, self).__init__()
            self.fc1 = nn.Linear(28 * 28, 200)
            self.fc2 = nn.Linear(200, 10)

        def forward(self, x):
            x = torch.relu(self.fc1(x))
            x = self.fc2(x)
            return x

    global_model = Net()
    return global_model

# Receive model updates from local nodes
def receive_local_updates():
    logging.info('Receiving local updates')
    local_updates = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12345))
        s.listen()

        for _ in range(NUM_LOCAL_NODES):
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                data = b''
                while True:
                    packet = conn.recv(4096)
                    if not packet:
                        break
                    data += packet
                local_update = pickle.loads(data)
                local_updates.append(local_update)

    return local_updates

# Aggregate local updates
def aggregate_updates(local_updates):
    logging.info('Aggregating local updates')
    aggregated_updates = np.mean(local_updates, axis=0)
    return aggregated_updates

# Update global model
def update_global_model(global_model, aggregated_updates):
    logging.info('Updating global model')
    for param, update in zip(global_model.parameters(), aggregated_updates):
        param.data = torch.Tensor(update)
    return global_model

# Broadcast global model
def send_global_model(global_model):
    logging.info('Broadcasting global model')
    serialized_model = pickle.dumps(global_model)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 12346))
        s.listen()

        for _ in range(NUM_LOCAL_NODES):
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                conn.sendall(serialized_model)

# Main function
def main():
    global_model = initialize_model()

    for round in range(NUM_ROUNDS):
        logging.info(f'Starting communication round {round+1}/{NUM_ROUNDS}')
        
        # 1. Receive model updates
        local_updates = receive_local_updates()
        
        # 2. Aggregate and update the global model
        aggregated_updates = aggregate_updates(local_updates)
        global_model = update_global_model(global_model, aggregated_updates)
        
        # 3. Wait for a minute
        time.sleep(60)
        
        # 4. Broadcast the updated global model to all nodes
        send_global_model(global_model)

if __name__ == "__main__":
    main()