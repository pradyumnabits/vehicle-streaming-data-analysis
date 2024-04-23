from faker import Faker
import random
from pymongo import MongoClient

# Initialize Faker library
fake = Faker()

# MongoDB configuration
mongo_host = "localhost"  # MongoDB host
mongo_port = 27017  # MongoDB port
mongo_db_name = "vehicle_movement_db"  # MongoDB database name
mongo_collection_name = "driver_profiles"  # MongoDB collection name for driver profiles

# Create MongoDB client
mongo_client = MongoClient(mongo_host, mongo_port)
mongo_db = mongo_client[mongo_db_name]
driver_profiles_collection = mongo_db[mongo_collection_name]

def generate_driver_profiles(count):
    driver_profiles = []
    for i in range(1, count + 1):
        driver_id = i
        driver_name = fake.name()
        age = random.randint(25, 60)
        phone = fake.phone_number()
        email = fake.email()
        address = fake.address().replace("\n", ", ")  # Address as a string
        experience_years = random.randint(1, 20)
        license_number = fake.random_int(min=100000, max=999999)  # 6-digit random number as license number

        driver_profile = {
            "driver_id": driver_id,
            "driver_name": driver_name,
            "age": age,
            "phone": phone,
            "email": email,
            "address": address,
            "experience_years": experience_years,
            "license_number": str(license_number)  # Convert to string for consistency
        }
        driver_profiles.append(driver_profile)

    return driver_profiles

def store_driver_profiles(driver_profiles):
    result = driver_profiles_collection.insert_many(driver_profiles)
    if result.inserted_ids:
        print("Driver profiles stored successfully.")
    else:
        print("Failed to store driver profiles.")

if __name__ == "__main__":
    # Generate 50 driver profiles
    driver_profiles_data = generate_driver_profiles(50)

    # Store driver profiles in MongoDB
    store_driver_profiles(driver_profiles_data)
