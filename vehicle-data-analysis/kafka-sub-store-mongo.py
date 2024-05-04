import json
import decimal
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from collections import defaultdict

# Kafka Broker details
kafka_broker = "localhost:9092"  # Change to your Kafka broker's address if needed
kafka_topic = "truck-data-kafka"  # Kafka topic to consume messages

# MongoDB configuration
mongo_host = "localhost"  # MongoDB host
mongo_port = 27017  # MongoDB port
mongo_db_name = "vehicle_movement_db"  # MongoDB database name
speed_metrics_routes = "speed_metrics_routes"  # MongoDB collection name for routes
speed_metrics_drivers = "speed_metrics_drivers"  # MongoDB collection name for drivers
SPEED_LIMIT = 80

# Create Kafka Consumer configuration
consumer_config = {
    "bootstrap.servers": kafka_broker,
    "group.id": "truck-data-consumer",
    "auto.offset.reset": "earliest"  # Start reading at the earliest message
}

# Create Kafka Consumer
consumer = Consumer(consumer_config)

# Create MongoDB client
mongo_client = MongoClient(mongo_host, mongo_port)
mongo_db = mongo_client[mongo_db_name]
mongo_collection_name = "speed_metrics"
speed_metrics_drivers_collection = mongo_db[speed_metrics_drivers]
speed_metrics_routes_collection = mongo_db[speed_metrics_routes]

# Data structure to store speeding statistics
mongo_collection = mongo_db[mongo_collection_name]
speeding_statistics_routes = defaultdict(lambda: {"count": 0, "last_timestamp": None})
speeding_statistics_drivers = defaultdict(lambda: {"count": 0, "last_timestamp": None})

def preprocess_data(trip_data):
    # Perform data cleansing and quality checks 
    processed_data = trip_data.copy()  # Create a copy to avoid modifying the original data

    # Handle missing values
    if processed_data.get("speed") is None:
        processed_data["speed"] = 0  # Default speed value if missing

    if processed_data.get("fuel_level") is None:
        processed_data["fuel_level"] = 50  # Default fuel level if missing

    if processed_data.get("tire_pressure") is None:
        processed_data["tire_pressure"] = 30  # Default tire pressure if missing

    # Handle missing values or data validity checks
    if processed_data["speed"] < 0:
        processed_data["speed"] = 0  # Set speed to 0 if negative
    
    # Quality Checks - Range Validations
    # Check speed range
    if processed_data["speed"] < 0:
        processed_data["speed"] = 0
    elif processed_data["speed"] > 150:
        processed_data["speed"] = 150  # Set maximum speed to 150

    # Check fuel level range
    if processed_data["fuel_level"] < 0:
        processed_data["fuel_level"] = 0
    elif processed_data["fuel_level"] > 100:
        processed_data["fuel_level"] = 100  # Set maximum fuel level to 100

    # Check tire pressure range
    if processed_data["tire_pressure"] < 20:
        processed_data["tire_pressure"] = 20  # Set minimum tire pressure
    elif processed_data["tire_pressure"] > 50:
        processed_data["tire_pressure"] = 50  # Set maximum tire pressure


    # Transformations - Convert Decimal values to float
    processed_data["speed"] = float(processed_data["speed"])
    processed_data["fuel_level"] = float(processed_data["fuel_level"])
    processed_data["tire_pressure"] = float(processed_data["tire_pressure"])
    processed_data["longitude"] = float(processed_data["longitude"])
    processed_data["latitude"] = float(processed_data["latitude"])

    # Perform additional quality checks and transformations as needed

    return processed_data


def process_trip_data(trip_data):
    try:
        #store_vehicle_movement_data(trip_data)
        if trip_data["speed"] > SPEED_LIMIT:  
            handle_speeding_incident(trip_data)
    except KeyError as e:
        print(f"KeyError: {e} in message: {trip_data}")


from datetime import datetime

def store_speeding_statistics(driver_id, driver_name, route, timestamp):
    # Convert timestamp string to datetime object
    timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    # Get the collection for storing speeding statistics
    speeding_statistics_collection = mongo_db["speeding_statistics"]

    # Insert the data into the collection with timestamp as datetime
    result = speeding_statistics_collection.insert_one({
        "driver_id": driver_id,
        "driver_name": driver_name,
        "route": route,
        "timestamp": timestamp_dt  # Store timestamp as datetime object
    })

    if result.inserted_id:
        print("Speeding statistics stored in the 'speeding_statistics' collection.")
    else:
        print("Failed to store speeding statistics.")


def handle_speeding_incident(trip_data):
    driver_id = trip_data["driver_id"]
    route = trip_data["route"]
    vehicle_id = trip_data["vehicle_id"]
    trip_id = trip_data["trip_id"]
    timestamp = trip_data["timestamp"]

    # Update speeding incidents count and timestamp for routes
    speeding_statistics_routes[route]["last_timestamp"] = timestamp

    # Update speeding incidents count and timestamp for drivers
    key = (driver_id, vehicle_id, trip_id)
    speeding_statistics_drivers[key]["last_timestamp"] = timestamp

    # Store speeding statistics
    store_speeding_statistics(driver_id, get_driver_name(driver_id), route, timestamp)
    store_speeding_statistics_routes(route)
    store_speeding_statistics_drivers(key)


def store_vehicle_movement_data(data):
    # Convert Decimal values to float for JSON serialization
    data["speed"] = float(data["speed"])
    data["fuel_level"] = float(data["fuel_level"])
    data["tire_pressure"] = float(data["tire_pressure"])
    data["longitude"] = float(data["longitude"])
    data["latitude"] = float(data["latitude"])

    # Get the collection for storing all data
    vehicle_movement_data = mongo_db["vehicle_movement_data"]

    # Insert the data into the collection
    result = vehicle_movement_data.insert_one(data)

    if result.inserted_id:
        print("Data stored in the 'vehicle_movement_data' collection.")
    else:
        print("Failed to store data in the collection.")

def store_speeding_statistics_routes(route):
    # Fetch the latest count for the route from the database
    existing_record = speed_metrics_routes_collection.find_one({"route": route})
    if existing_record:
        count = existing_record.get("speeding_count", 0)
        last_timestamp = existing_record.get("last_timestamp")
    else:
        count = 0
        last_timestamp = None

    # Update the count in the database
    speed_metrics_routes_collection.update_one(
        {"route": route},
        {
            "$set": {
                "speeding_count": count + 1,
                "last_timestamp": speeding_statistics_routes[route]["last_timestamp"]
                if route in speeding_statistics_routes
                else last_timestamp
            }
        },
        upsert=True
    )


def store_speeding_statistics_drivers(key):
    driver_id, vehicle_id, trip_id = key

    # Fetch the latest count for the driver from the database
    existing_record = speed_metrics_drivers_collection.find_one({"driver_id": driver_id})
    if existing_record:
        count = existing_record.get("speeding_count", 0)
        last_timestamp = existing_record.get("last_timestamp")
    else:
        count = 0
        last_timestamp = None

    # Update the count in the database
    speed_metrics_drivers_collection.update_one(
        {"driver_id": driver_id},
        {
            "$set": {
                "speeding_count": count + 1,
                "last_timestamp": speeding_statistics_drivers[key]["last_timestamp"]
                if key in speeding_statistics_drivers
                else last_timestamp,
                "driver_name": get_driver_name(driver_id)  # Get driver name from driver_profiles collection
            }
        },
        upsert=True
    )

def get_driver_name(driver_id):
    # Query the driver_profiles collection to get the driver's name based on driver_id
    driver_profile = mongo_db["driver_profiles"].find_one({"driver_id": driver_id})
    print(f"Driver ID: {driver_id}, Driver Profile: {driver_profile}")

    if driver_profile:
        return driver_profile.get("driver_name", "Unknown")
    else:
        return "Unknown"


# Subscribe to Kafka topic
consumer.subscribe([kafka_topic])

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition
                print(f"Reached end of partition {msg.partition()} offset {msg.offset()}")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Process the received message
            message_data = msg.value().decode('utf-8')
            try:
                trip_data = json.loads(message_data, parse_float=decimal.Decimal)
                # Pre-process trip data
                processed_trip_data = preprocess_data(trip_data)
                # Process trip data
                process_trip_data(processed_trip_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON message: {message_data}")
            except Exception as e:
                print(f"Error processing message: {message_data}, Exception: {e}")

except KeyboardInterrupt:
    pass

finally:
    # Clean up
    consumer.close()
    mongo_client.close()
