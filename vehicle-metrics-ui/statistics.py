import logging
from flask import Flask, render_template, request
from pymongo import MongoClient
import plotly.graph_objs as go
from datetime import datetime, timedelta
import json
from pymongo import DESCENDING 

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['vehicle_movement_db']
speed_metrics_routes_collection = db['speed_metrics_routes']
speed_metrics_drivers_collection = db['speed_metrics_drivers']
driver_profiles_collection = db['driver_profiles']
vehicle_movement_data = db['vehicle_movement_data']
speed_metrics_collection = db['speeding_statistics']


@app.route('/')
def index():
    # Placeholder index page
    return render_template('index.html')

from datetime import datetime, timedelta

# Function to get the timestamp for the specified hours ago
def get_timestamp(hours):
    return datetime.now() - timedelta(hours=hours)

@app.route('/drivers/profiles')
def driver_profiles_endpoint():
    data = list(driver_profiles_collection.find())
    logging.debug(f"Retrieved {len(data)} documents from driver_profiles collection.")
    return render_template('driver_profiles.html', data=data)

@app.route('/metrics/routes')
def speed_metrics_routes_endpoint():
    # Get the hours filter from query parameters, default to 1 if not provided
    hours = int(request.args.get('hours', 1))

    # Log the MongoDB aggregation pipeline
    pipeline = [
        {"$match": {"timestamp": {"$gt": get_timestamp(hours)}}},
        {"$group": {"_id": {"route": "$route"}, "total_speeding_count": {"$sum": 1}}},
        {"$sort": {"total_speeding_count": -1}}
    ]

    logging.debug(f"MongoDB aggregation pipeline for speed_metrics_routes collection: {pipeline}")

    # Aggregate data for routes based on timestamp
    data = list(speed_metrics_collection.aggregate(pipeline))

    logging.debug(f"Retrieved {len(data)} documents from speed_metrics collection for the last {hours} hour(s).")

    # Format the data for rendering
    formatted_data = [{"route": item["_id"]["route"], "total_speeding_count": item["total_speeding_count"]} for item in data]

    # Convert data to JSON
    data_json = json.dumps(formatted_data)
    print(data_json)


    return render_template('speed_metrics_routes.html', data=formatted_data)


@app.route('/metrics/drivers')
def speed_metrics_drivers_endpoint():
    # Get the hours filter from query parameters, default to 1 if not provided
    hours = int(request.args.get('hours', 1))

    pipeline = [
        {"$match": {"timestamp": {"$gt": get_timestamp(hours)}}},
        {"$group": {"_id": {"driver_id": "$driver_id", "driver_name": "$driver_name"}, "total_speeding_count": {"$sum": 1}}},
        {"$sort": {"total_speeding_count": -1}}
    ]

    
    logging.debug(f"MongoDB aggregation pipeline for speed_metrics collection: {pipeline}")

    # Aggregate data for drivers based on timestamp
    data = list(speed_metrics_collection.aggregate(pipeline))

    logging.debug(f"Retrieved {len(data)} documents from speed_metrics collection for the last {hours} hour(s).")

    # Format the data for rendering
    formatted_data = [{"driver_id": item["_id"]["driver_id"], "driver_name": item["_id"]["driver_name"], "total_speeding_count": item["total_speeding_count"]} for item in data]

    # Convert data to JSON
    data_json = json.dumps(formatted_data)

    # Print the JSON data
    print(data_json)

    return render_template('speed_metrics_drivers.html', data=formatted_data)


# @app.route('/vehicle/data')
# def vehicle_movement_data_endpoint():
#     # Assuming vehicle_movement_data_collection is your MongoDB collection
#     data = list(vehicle_movement_data.find())
#     logging.debug(f"Retrieved {len(data)} documents from vehicle_movement_data collection.")
#     return render_template('vehicle_movement_data.html', data=data)

@app.route('/vehicle/data')
def vehicle_movement_data_endpoint():
    # Assuming vehicle_movement_data_collection is your MongoDB collection
    data = list(vehicle_movement_data.find().sort('_id', DESCENDING).limit(50))  # Sort by _id in descending order and limit to 50 records
    logging.debug(f"Retrieved {len(data)} documents from vehicle_movement_data collection.")
    return render_template('vehicle_movement_data.html', data=data)




if __name__ == '__main__':
    app.run(debug=True)
