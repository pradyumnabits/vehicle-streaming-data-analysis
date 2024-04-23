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
mongo_db_name = "truck_trip_data_db"  # MongoDB database name
mongo_collection_name = "speed_metrics"  # MongoDB collection name

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
mongo_collection = mongo_db[mongo_collection_name]

# Data structure to store speeding statistics
speeding_statistics = defaultdict(lambda: {"count": 0, "last_timestamp": None})

# def process_trip_data(trip_data):
#     try:
#         # Check if the speed exceeds the limit
#         if trip_data["speed"] > 100:  # Assuming 100 as the speed limit
#             handle_speeding_incident(trip_data)
#     except KeyError as e:
#         print(f"KeyError: {e} in message: {trip_data}")

# def handle_speeding_incident(trip_data):
#     driver_id = trip_data["driver_id"]
#     route = trip_data["route"]
#     vehicle_id = trip_data["vehicle_id"]
#     timestamp = trip_data["timestamp"]

#     # Update speeding incidents count and timestamp
#     speeding_statistics[(driver_id, route, vehicle_id)]["count"] += 1
#     speeding_statistics[(driver_id, route, vehicle_id)]["last_timestamp"] = timestamp
#     store_speeding_statistics()

# def store_speeding_statistics():
#     # Store speeding statistics in MongoDB
#     for key, value in speeding_statistics.items():
#         driver_id, route, vehicle_id = key
#         count = value["count"]
#         last_timestamp = value["last_timestamp"]
#         mongo_collection.insert_one({
#             "driver_id": driver_id,
#             "route": route,
#             "vehicle_id": vehicle_id,
#             "speeding_count": count,
#             "last_timestamp": last_timestamp
#         })

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
                print("Received Kafka message_data:", message_data) 
                if not message_data.startswith('{') or not message_data.endswith('}'):
                    raise ValueError("Invalid JSON format")

                   
                # Parse JSON with Decimal handling
                trip_data = json.loads(message_data, parse_float=decimal.Decimal)

                #trip_data = json.loads(message_data)

                print("Received Kafka message_data:", trip_data)

                #process_trip_data(trip_data)
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
