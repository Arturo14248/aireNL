import os

from dotenv import load_dotenv
from openaq import OpenAQ

# Load variables from .env file into os.environ
load_dotenv()

# Access the variables
api_key = os.getenv("API_KEY")

client = OpenAQ(api_key=api_key)

try:
    response = client.locations.list(
        coordinates=(25.68435, -100.31721), radius=12000, limit=100
    )

    print(f"Locations found: {response.meta.found}\n")

    for location in response.results:
        print(f"ID: {location.id}")
        print(f"Name: {location.name}")
        print(f"Coordinates: {location.coordinates}")

        for sensor in location.sensors:
            print("Sensor ID:", sensor.id)
            print("Sensor name:", sensor.name)
            print("Parameter:", sensor.parameter)

            print("All sensor fields:")
            for field in sensor.__slots__:
                print(f"  {field}: {getattr(sensor, field)}")
        print("-" * 40)

finally:
    client.close()
