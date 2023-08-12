from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initial data
data = {
    'car_id': 'ABC789',
    'model': 'Model W',
    'current_speed': 75,  # km/h
    'battery_capacity': 50,  # kWh  
    'charge': 100,  # %  (percentage)
    'consumption': 0.17,  # kWh/1 km  
    'engine_power': 110,  # kW
    'engine_torque': 220,  # Nm
    'location': 149,  # km
    'node': 'Node 1',
    'charging': False,
    'distance_covered': 0,
}

# Time step in minutes
time_step = 0.1

# Distance covered in each time step (in km)
distance_step = data['current_speed'] * (time_step/60)

charging_stations = [150, 270, 460]  # Charging station locations

for i in range(int(600/distance_step)):
    if data['charge'] <= 0:
        break

    # If the car is charging
    if data['charging']:
        # Increase charge
        data['charge'] += 30  # 30% charge per 30 minutes

        # Randomly decide to stop charging
        if random.random() < 0.5:  # 50% chance to stop charging each time step
            data['charging'] = False

    # If the car is not charging
    else:
        # Update location
        data['location'] += distance_step
        data['distance_covered'] += distance_step

        # Update node based on location
        if data['location'] < 150:
            data['node'] = 'Node 1'
        elif data['location'] < 270:
            data['node'] = 'Node 2'
        elif data['location'] < 460:
            data['node'] = 'Node 3'
        else:
            data['node'] = 'Node 4'

        # Update charge based on consumption
        data['charge'] -= data['consumption'] * distance_step / data['battery_capacity'] * 100

        # If at a charging station and charge is low, start charging
        if data['location'] in charging_stations and data['charge'] <= 30:  # Start charging if charge is 30% or less
            data['charging'] = True

    # Print current status
    print(f"Time step: {i*time_step} minutes")
    print(f"Current speed: {data['current_speed']} km/h")
    print(f"Distance covered in this step: {distance_step} km")
    print(f"Charge consumed in this step: {data['consumption'] * distance_step} %")
    print(f"Remaining charge: {data['charge']} %")
    print(f"Total distance covered: {data['distance_covered']} km")
    print("--------------------------")

    # Send data to Kafka
    producer.send('electric_car_data', data)
    if data['node'] == 'Node 1':
        producer.send('node1_data', data)
    elif data['node'] == 'Node 2':
        producer.send('node2_data', data)
    elif data['node'] == 'Node 3':
        producer.send('node3_data', data)
    else:
        producer.send('node4_data', data)

    # Wait for the next time step
    time.sleep(time_step * 60)  # convert to seconds
