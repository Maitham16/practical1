from kafka import KafkaProducer
import json
import time
import random
import datetime

# Constants
BASE_SPEED = 90  # km/h
CONSUMPTION_MODIFIER = 0.002  # arbitrary factor to simulate increased consumption at higher speeds
CHARGING_STATIONS = [150, 270, 460]  # Charging station locations
BATTERY_LIFE_DEGRADATION_RATE = 0.001 # % per km
DEGRADATION_FACTOR = 0.5    # arbitrary factor to simulate increased consumption at lower battery life

SIMULATION_SPEED_FACTOR = 60  # 1 = real time, 60 = 1 hour per second, 3600 = 1 day per second 
real_start_time = datetime.datetime.now()
simulated_start_time = datetime.datetime.now()

def create_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_simulated_timestamp(real_start_time, simulated_start_time, speed_factor):
    real_elapsed = datetime.datetime.now() - real_start_time
    simulated_elapsed = real_elapsed * speed_factor
    return simulated_start_time + simulated_elapsed

def update_car_data_while_charging(data):
    data['charge'] += 30  # 30% charge per 30 minutes
    if data['charge'] > 100:   # Ensure charging does not exceed 100%
        data['charge'] = 100
    if random.random() < 0.5:  # 50% chance to stop charging each time step
        data['charging'] = False

def update_car_data_while_moving(data, distance_step):
    # Updating location, distance covered, and current speed
    data['location'] += distance_step # km
    data['distance_covered'] += distance_step # km
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


            if data['charge'] >= charge_required_to_reach_station:
                data['location'] = next_station
                data['charging'] = True
            else:
                distance_possible = data['charge'] / data['consumption'] * data['battery_capacity'] / 100
                data['location'] += distance_possible
                data['charge'] = 0
                data['charging'] = False

    # Randomly increase or decrease the current speed
    speed_variation = 10 # km/h
    min_speed = 10
    max_speed = 180
    data['current_speed'] = random.randint(max(data['current_speed'] - speed_variation, min_speed), 
                                           min(data['current_speed'] + speed_variation, max_speed))

    # Update the node based on the current location
    if data['location'] < 150:
        data['node'] = 'Node 1'
    elif data['location'] < 280:
        data['node'] = 'Node 2'
    elif data['location'] < 420:
        data['node'] = 'Node 3'
    elif data['location'] < 600:
        data['node'] = 'Node 4'
    else:
        print("Car has reached its destination.")

    return modified_consumption

def main():
    producer = create_producer()

    # Initial data
    data = {
        'car_id': 'car12', # unique identifier for each car
        'model': 'Model Q', # car model
        'current_speed': 55,  # km/h
        'battery_capacity': 45,  # kWh  
        'charge': 76,  # %  (percentage)
        'consumption': 0.14,  # kWh/1 km  
        'engine_power': 120,  # kW
        'engine_torque': 240,  # Nm
        'location': 297,  # km
        'node': 'Node 3', # node the car is currently in
        'charging': False, # whether the car is currently charging
        'distance_covered': 0, # km
        'battery_life': 98  # %, 100 being a brand new battery
    }

    time_step = 1 # minutes per time step
    distance_step = data['current_speed'] * (time_step/60) # km per time step
    counter = 0 # time step counter

    while data['charge'] > 0 and data['distance_covered'] < 600:
        counter += 1
        
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

        data['timestamp'] = get_simulated_timestamp(real_start_time, simulated_start_time, SIMULATION_SPEED_FACTOR).strftime('%Y-%m-%d %H:%M:%S')

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
        
        time.sleep(time_step * 60 / SIMULATION_SPEED_FACTOR)

if __name__ == "__main__":
    main()