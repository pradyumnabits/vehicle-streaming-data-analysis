import json
import time
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from pymongo import MongoClient

# MQTT Broker details
mqtt_broker_address = "localhost"  # Change to your MQTT broker's address if needed
mqtt_port = 1883  # Default MQTT port
mqtt_topic = "truck-data-topic"  # MQTT topic to subscribe

# Kafka Producer configuration
kafka_broker = "localhost:9092"  # Change to your Kafka broker's address if needed
kafka_topic = "truck-data-kafka"  # Kafka topic to produce messages

# MongoDB configuration
mongo_host = "localhost"  # MongoDB host
mongo_port = 27017  # MongoDB port
mongo_db_name = "vehicle_movement_db"  # MongoDB database name

# Create Kafka Producer
kafka_producer = Producer({"bootstrap.servers": kafka_broker})

# Callback function for Kafka delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to Kafka topic: {msg.topic()}")

# Create MQTT client
mqtt_client = mqtt.Client("MQTT-to-Kafka-Client")

# Callback function for MQTT on_message event
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print("Received MQTT message:", payload)
    
    try:
        # Assuming payload is JSON formatted
        payload_dict = json.loads(payload)
        
        # Convert longitude and latitude to float
        payload_dict['longitude'] = float(payload_dict['longitude'])
        payload_dict['latitude'] = float(payload_dict['latitude'])
        
        # Additional processing or validation of payload_dict if needed
        
        # Convert payload_dict back to JSON string for Kafka
        kafka_payload = json.dumps(payload_dict)
        
        # Send the payload to Kafka
        kafka_producer.produce(kafka_topic, kafka_payload.encode("utf-8"), callback=delivery_report)
        kafka_producer.poll(0)
        
        # Store data in MongoDB
        store_vehicle_movement_data(payload_dict)
        
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON message: {str(e)}")
    except Exception as e:
        print(f"Error processing message: {str(e)}")

def store_vehicle_movement_data(data):
    # Convert Decimal values to float for JSON serialization
    data["speed"] = float(data["speed"])
    data["fuel_level"] = float(data["fuel_level"])
    data["tire_pressure"] = float(data["tire_pressure"])
    data["longitude"] = float(data["longitude"])
    data["latitude"] = float(data["latitude"])

    # Connect to MongoDB and store data
    mongo_client = MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[mongo_db_name]
    vehicle_movement_data = mongo_db["vehicle_movement_data"]

    # Insert the data into the collection
    result = vehicle_movement_data.insert_one(data)

    if result.inserted_id:
        print("Data stored in the 'vehicle_movement_data' collection.")
    else:
        print("Failed to store data in the collection.")

# Set MQTT callback function
mqtt_client.on_message = on_message

# Connect to MQTT Broker
mqtt_client.connect(mqtt_broker_address, mqtt_port)
mqtt_client.subscribe(mqtt_topic)
mqtt_client.loop_start()

# Keep the program running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

# Clean up
mqtt_client.loop_stop()
mqtt_client.disconnect()
kafka_producer.flush()
