from kafka import KafkaProducer
import json
import time
import random

# Constants
BASE_SPEED = 90  # km/h at which the given consumption rate is measured
CONSUMPTION_MODIFIER = 0.002  # arbitrary factor, adjust for realism
CHARGING_STATIONS = [150, 270, 460]  # Charging station locations
BATTERY_LIFE_DEGRADATION_RATE = 0.001
DEGRADATION_FACTOR = 0.5

def create_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def update_car_data_while_charging(data):
    data['charge'] += 30  # 30% charge per 30 minutes
    if data['charge'] > 100:   # Ensure charging does not exceed 100%
        data['charge'] = 100
    if random.random() < 0.5:  # 50% chance to stop charging each time step
        data['charging'] = False

def update_car_data_while_moving(data, distance_step):
    # Updating location, distance covered, and current speed
    data['location'] += distance_step
    data['distance_covered'] += distance_step
    speed_factor = data['current_speed'] / BASE_SPEED
    adjusted_consumption = data['consumption'] * speed_factor + (CONSUMPTION_MODIFIER * (speed_factor - 1))
    modified_consumption = adjusted_consumption * (1 + ((100 - data['battery_life']) / 100) * DEGRADATION_FACTOR)
    data['charge'] -= modified_consumption * distance_step / data['battery_capacity'] * 100
    data['battery_life'] -= BATTERY_LIFE_DEGRADATION_RATE
    
    # Check if the current location is approaching a charging station and if charge is low
    if data['charge'] <= 30:
        next_station = next((station for station in CHARGING_STATIONS if station > data['location']), None)
        if next_station:
            distance_to_next_station = next_station - data['location']
            charge_required_to_reach_station = distance_to_next_station / data['battery_capacity'] * data['consumption'] * 100

            # If there's enough charge to reach the station, then update location and start charging
            if data['charge'] >= charge_required_to_reach_station:
                data['location'] = next_station
                data['charging'] = True
            else:
                # If not enough charge to reach the station, move as far as possible and set charge to 0
                distance_possible = data['charge'] / data['consumption'] * data['battery_capacity'] / 100
                data['location'] += distance_possible
                data['charge'] = 0
                data['charging'] = False

    # Simulating slight variations in speed
    speed_variation = 10
    min_speed = 10
    max_speed = 180
    data['current_speed'] = random.randint(max(data['current_speed'] - speed_variation, min_speed), 
                                           min(data['current_speed'] + speed_variation, max_speed))

    # Update the node based on the current location
    if data['location'] < 150:
        data['node'] = 'Node 1'
    elif data['location'] < 220:
        data['node'] = 'Node 2'
    elif data['location'] < 300:
        data['node'] = 'Node 3'
    elif data['location'] < 400:
        data['node'] = 'Node 4'
    elif data['location'] < 500:
        data['node'] = 'Node 5'
    elif data['location'] < 600:
        data['node'] = 'Node 6'
    else:
        data['node'] = 'Node 7'

    return modified_consumption

def main():
    producer = create_producer()

    # Initial data
    data = {
        'car_id': 'car8',
        'model': 'Model R',
        'current_speed': 60,  # km/h
        'battery_capacity': 40,  # kWh  
        'charge': 75,  # %  (percentage)
        'consumption': 0.13,  # kWh/1 km  
        'engine_power': 120,  # kW
        'engine_torque': 210,  # Nm
        'location': 20,  # km
        'node': 'Node 1',
        'charging': False,
        'distance_covered': 0,
        'battery_life': 100  # %, 100 being a brand new battery
    }

    time_step = 1
    distance_step = data['current_speed'] * (time_step/60)

    counter = 0

    while data['charge'] > 0 and data['distance_covered'] < 600:
        counter += 1  # Increment counter with each loop iteration
        
        if data['charging']:
            update_car_data_while_charging(data)
        else:
            modified_consumption = update_car_data_while_moving(data, distance_step)
        
        # Recalculate the distance_step based on current speed
        distance_step = data['current_speed'] * (time_step/60)

        # Ensure charge does not go negative
        if data['charge'] < 0:
            data['charge'] = 0
            print("Car has run out of charge and has stopped.")
            break

        # Print current status
        print(f"Time step: {counter*time_step} minutes")
        print(f"Current speed: {data['current_speed']} km/h")
        print(f"Distance covered in this step: {distance_step} km")
        print(f"Charge consumed in this step: {modified_consumption * distance_step} %")
        print(f"Remaining charge: {data['charge']} %")
        print(f"Total distance covered: {data['distance_covered']} km")
        print(f"Battery life: {data['battery_life']} %")
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
        
        # Sleep before moving to the next step
        time.sleep(time_step * 60)  # convert to seconds

if __name__ == "__main__":
    main()