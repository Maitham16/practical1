import csv
import json
import joblib
import numpy as np
import tensorflow as tf
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Global variables to keep track of total predictions and correct predictions
total_predictions = 0
correct_predictions = 0

# Load Random Forest model
rf_model = joblib.load('/home/maith/Desktop/practical1/random_forest_model_node_2.pkl')

# Load Neural Network model
nn_model = tf.keras.models.load_model('/home/maith/Desktop/practical1/neural_network_model_node_2.h5')

# Load scaler object
scaler = joblib.load('/home/maith/Desktop/practical1/scaler_node_2.pkl')

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
    return features

def predict_and_print(data):
    global total_predictions
    global correct_predictions
    
    try:
        data['needs_charge'] = 1 if float(data['charge']) <= 50 else 0
        features = process_data(data)
        prediction_rf = predict_need_charge(rf_model, scaler, features)
        prediction_nn = predict_need_charge(nn_model, scaler, features)
        print(f"Random Forest Prediction: {prediction_rf}")
        print(f"Neural Network Prediction: {prediction_nn}")
        
        # Update total_predictions and correct_predictions
        total_predictions += 1
        if prediction_rf == data['needs_charge']:
            correct_predictions += 1
        
        # Compute running accuracy
        accuracy = correct_predictions / total_predictions
        
        # Update plot
        plt.plot(total_predictions, accuracy, 'bo')
        plt.xlabel('Total Predictions')
        plt.ylabel('Accuracy')
        plt.title('Model Accuracy Over Time')
        plt.pause(0.05)
        
    except Exception as e:
        print(f"Error predicting data: {e}")
        raise

def predict_need_charge(model, scaler, features):
    # scale the features
    features_scaled = scaler.transform(np.array(features).reshape(1, -1))
    # make prediction
    prediction = model.predict(features_scaled)
    return int(prediction.round())

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
                predict_and_print(data)
        except KeyboardInterrupt:
            pass
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if file:
            file.close()

if __name__ == "__main__":
    plt.ion()
    start_consumer('node2_data', 'node2_data.csv')
    plt.ioff()  # Turn off interactive mode
    plt.show()