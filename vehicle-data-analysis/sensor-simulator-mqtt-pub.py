from faker import Faker
import random
import time
import paho.mqtt.client as mqtt
import json
from datetime import datetime  # Import datetime module

# Initialize Faker library
fake = Faker()

# Create a pool of trip ID, driver ID, and vehicle ID combinations
trip_driver_vehicle_pool = []
for _ in range(100):  # Adjust the range as needed
    trip_id = fake.uuid4()
    driver_id = random.randint(1, 10)
    vehicle_id = driver_id  # Ensure the vehicle ID is the same as the driver ID
    trip_driver_vehicle_pool.append((trip_id, driver_id, vehicle_id))

# MQTT Broker details
broker_address = "localhost"  # Change to your MQTT broker's address if needed
port = 1883  # Default MQTT port
topic = "truck-data-topic"

# Create MQTT client
client = mqtt.Client("Simulator")

# Connect to MQTT Broker
client.connect(broker_address, port=port)


def generate_trip_data():
    # Get a random combination from the pool
    trip_id, driver_id, vehicle_id = random.choice(trip_driver_vehicle_pool)

    # Create a pool of 20 different routes
    route_pool = [
        "Main Street",
        "Broadway Avenue",
        "Park Avenue",
        "Maple Avenue",
        "Sunset Boulevard",
        "Oak Street",
        "Pine Street",
        "Elm Street",
        "River Road",
        "Lakeview Drive",
        "Hillcrest Drive",
        "Meadow Lane",
        "Valley Road",
        "Forest Lane",
        "Cedar Avenue",
        "Willow Lane",
        "Highland Drive",
        "Washington Street",
        "Lincoln Avenue",
        "Smith Street",
    ]

    trip_data = {
        "driver_id": driver_id,
        "vehicle_id": vehicle_id,
        "route": random.choice(route_pool),
        "speed": random.uniform(60, 120),
        "trip_id": trip_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Use current timestamp
        "longitude": float(fake.longitude()),  # Convert Decimal to float
        "latitude": float(fake.latitude()),  # Convert Decimal to float
        "fuel_level": random.uniform(0, 100),
        "tire_pressure": random.uniform(25, 45),
        "driver_condition": random.choice(["Alert", "Fatigued", "Distracted"]),
        "incident_type": random.choice(["None", "Speeding", "Harsh Braking", "Collision"]),
        "weather_condition": random.choice(["Clear", "Rainy", "Snowy"]),
        "traffic_condition": random.choice(["Light", "Moderate", "Heavy"])
    }
    
    return trip_data

def publish_data(data):
    # Publish data to MQTT topic
    json_data = json.dumps(data)
    print("Published data to MQTT broker: ", json_data)
    client.publish(topic, json_data)

def simulate_data():
    while True:
        data = generate_trip_data()
        publish_data(data)
        time.sleep(1)  # Adjust delay as needed

if __name__ == "__main__":
    simulate_data()
