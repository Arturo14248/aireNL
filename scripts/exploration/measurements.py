import os

from dotenv import load_dotenv
from openaq import OpenAQ

# Load variables from .env file into os.environ
load_dotenv()

# Access the variables
api_key = os.getenv("OPENAQ_API_KEY")


client = OpenAQ(api_key=api_key)

try:
    response = client.measurements.list(
        sensors_id=14189968,
        data="days",
        limit=100,
        date_from="2026-11-01",
        date_to="2026-11-16",
    )

    print(f"Measurements found: {response.meta.found}\n")
    for measurement in response.results:
        print(
            f"Value: {measurement.value} | \n"
            f"Units: {measurement.parameter.units} | \n"
            f"Date aggregations from: {measurement.period.datetime_from} | \n"
            f"Date aggregations to: {measurement.period.datetime_to} | "
        )
        print("-" * 40)
finally:
    client.close()
