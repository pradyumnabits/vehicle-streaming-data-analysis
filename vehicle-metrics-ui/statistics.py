import logging
from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import plotly.graph_objs as go


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

@app.route('/')
def index():
    # Placeholder index page
    return render_template('index.html')


@app.route('/drivers/profiles')
def driver_profiles_endpoint():
    data = list(driver_profiles_collection.find())
    logging.debug(f"Retrieved {len(data)} documents from driver_profiles collection.")
    return render_template('driver_profiles.html', data=data)


@app.route('/metrics/routes')
def speed_metrics_routes_endpoint():
    # Assuming speed_metrics_routes_collection is your MongoDB collection
    # data = list(speed_metrics_routes_collection.find())
    data = list(speed_metrics_routes_collection.find().sort("speeding_count", -1))
    logging.debug(f"Retrieved {len(data)} documents from speed_metrics_routes collection.")
    return render_template('speed_metrics_routes.html', data=data)


@app.route('/metrics/drivers')
def speed_metrics_drivers_endpoint():
    # Assuming speed_metrics_drivers_collection is your MongoDB collection
    # data = list(speed_metrics_drivers_collection.find())
    data = list(speed_metrics_drivers_collection.find().sort("speeding_count", -1))
    logging.debug(f"Retrieved {len(data)} documents from speed_metrics_drivers collection.")
    return render_template('speed_metrics_drivers.html', data=data)

@app.route('/vehicle/data')
def vehicle_movement_data_endpoint():
    # Assuming vehicle_movement_data_collection is your MongoDB collection
    data = list(vehicle_movement_data.find())
    logging.debug(f"Retrieved {len(data)} documents from vehicle_movement_data collection.")
    return render_template('vehicle_movement_data.html', data=data)


@app.route('/metrics/routes/chart')
def speed_metrics_routes_chart_endpoint():
    # Fetch data from MongoDB
    routes_data = list(speed_metrics_routes_collection.find())

    # Prepare data for plotting
    route_names = [route['route'] for route in routes_data]
    speeding_counts = [route['speeding_count'] for route in routes_data]

    # Create a bar chart using Plotly
    fig = go.Figure()
    fig.add_trace(go.Bar(x=route_names, y=speeding_counts, name='Speeding Counts'))

    # Set layout options
    fig.update_layout(title='Speeding Statistics by Route', xaxis_title='Route', yaxis_title='Speeding Counts')

    # Convert the Plotly figure to JSON for embedding in HTML
    chart_json = fig.to_json()

    return render_template('speeding_routes_chart.html', chart_json=chart_json)


if __name__ == '__main__':
    app.run(debug=True)
