import time
from collections import defaultdict

# Example data structure to store speeding incidents
speeding_incidents = defaultdict(lambda: {"count": 0, "last_timestamp": None})

def check_for_speed_limit_crossing(trip_data):
    # Check if the speed exceeds the limit
    if trip_data["speed"] > 100:  # Assuming 100 as the speed limit
        handle_speeding_incident(trip_data)

def handle_speeding_incident(trip_data):
    driver_id = trip_data["driver_id"]
    route = trip_data["route"]
    vehicle_id = trip_data["vehicle_id"]
    timestamp = trip_data["timestamp"]

    # Update speeding incidents count and timestamp
    speeding_incidents[(driver_id, route, vehicle_id)]["count"] += 1
    speeding_incidents[(driver_id, route, vehicle_id)]["last_timestamp"] = timestamp

def print_speeding_statistics():
    print("Speeding Statistics:")
    for key, value in speeding_incidents.items():
        driver_id, route, vehicle_id = key
        count = value["count"]
        last_timestamp = value["last_timestamp"]
        print(f"Driver ID: {driver_id}, Route: {route}, Truck ID: {vehicle_id}, Speeding Count: {count}, Last Timestamp: {last_timestamp}")

# Example trip data
trip_data1 = {
    "driver_id": 1,
    "vehicle_id": 1,
    "trip_id": 1,
    "timestamp": "2024-04-23T12:34:56",
    "longitude": 123.456,
    "latitude": 45.678,
    "speed": 110,  # Exceeds speed limit
    "fuel_level": 70,
    "tire_pressure": 30,
    "driver_condition": "Alert",
    "incident_type": "Speeding",
    "route": "Highway A",
    "weather_condition": "Clear",
    "traffic_condition": "Light"
}

trip_data2 = {
    "driver_id": 2,
    "vehicle_id": 2,
    "trip_id": 2,
    "timestamp": "2024-04-24T09:12:34",
    "longitude": 111.222,
    "latitude": 33.444,
    "speed": 90,  # Below speed limit
    "fuel_level": 60,
    "tire_pressure": 35,
    "driver_condition": "Fatigued",
    "incident_type": "None",
    "route": "Highway B",
    "weather_condition": "Rainy",
    "traffic_condition": "Moderate"
}

# Preprocess data and handle speeding incidents
check_for_speed_limit_crossing(trip_data1)
check_for_speed_limit_crossing(trip_data2)

# Print speeding statistics
print_speeding_statistics()
