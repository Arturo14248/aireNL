# OpenAQ API — Python Basics

A practical guide to querying air-quality data using the OpenAQ Python SDK.

## 1\. Introduction

OpenAQ provides access to air-quality monitoring data from stations around the world.

You can use the API to retrieve:

- Monitoring locations
- Sensors associated with locations
- Pollutant measurements, such as PM2.5, PM10, NO₂, and O₃
- Measurement timestamps and values
- Metadata about API responses

The typical workflow is:

1. Connect to the API.
2. Query locations.
3. Inspect the returned data.
4. Find the sensors associated with each location.
5. Query measurements.
6. Export and analyze the results.

## 2\. Installation

Install the Python SDK:

bash

```
pip install openaq
```

Import the client:

python

```
from openaq import OpenAQ
```

## 3\. Authentication

You need an OpenAQ API key to make requests.

python

```
from openaq import OpenAQ

client = OpenAQ(api_key="YOUR_API_KEY")
```

Replace `YOUR_API_KEY` with your actual API key.

### Recommended: Use an environment variable

Avoid hardcoding your API key in scripts.

Set the environment variable in your terminal:

bash

```
export OPENAQ_API_KEY="YOUR_API_KEY"
```

Then load it in Python:

python

```
import os
from openaq import OpenAQ

client = OpenAQ(api_key=os.environ["OPENAQ_API_KEY"])
```

## 4\. Query locations

A location represents a place where air quality is monitored.

Example: search for monitoring locations around Nashville, Tennessee.

python

```
response = client.locations.list(
    coordinates=(36.1627, -86.7816),
    radius=12000,
    limit=100
)
```

Parameters:

| Parameter | Description |
| --- | --- |
| `coordinates` | Coordinates of the search center. Verify the expected coordinate order for your SDK version. |
| `radius` | Search radius in meters, where supported. |
| `limit` | Maximum number of results requested per page. |

Print the response:

python

```
print(response)
```

## 5\. Understand the API response

The response contains metadata and a collection of results.

python

```
print("Metadata:", response.meta)
print("Results:", response.results)
print("Number returned:", len(response.results))
print("Total found:", response.meta.found)
```

Important attributes:

- `response.meta`: response metadata, including pagination information.
- `response.results`: list of returned location objects.
- `response.meta.found`: reported number of matching records, when provided.

**Important:** The number of records returned in one response may differ from the total number of matching records.

## 6\. Print all fields

The `Location` objects returned by my installed SDK do not support `model_dump()`.

Instead, use `vars()` when the object exposes its attributes through `__dict__`.

python

```
import json

for location in response.results:
    print(json.dumps(vars(location), indent=2, default=str))
```

This prints the object's available attributes and values as formatted JSON.

### Create a reusable conversion function

Different Python models may expose their data differently. This helper supports several common cases:

python

```
def to_dict(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump()

    if hasattr(obj, "dict"):
        return obj.dict()

    if hasattr(obj, "__dict__"):
        return vars(obj)

    return str(obj)
```

Use it to print the results:

python

```
import json

for location in response.results:
    print(json.dumps(to_dict(location), indent=2, default=str))
```

## 7\. Discover available fields

Use `dir()` to inspect the attributes and methods available on an object.

python

```
if response.results:
    location = response.results[0]

    print(dir(location))
else:
    print("No locations found.")
```

To display public attributes and their values:

python

```
if response.results:
    location = response.results[0]

    for name in dir(location):
        if name.startswith("_"):
            continue

        try:
            value = getattr(location, name)

            if not callable(value):
                print(f"{name}: {value}")

        except Exception:
            pass
```

### Difference between `dir()` and `vars()`

- `dir(obj)` lists attributes and methods available on an object.
- `vars(obj)` returns its instance attribute dictionary, if available.
- `getattr(obj, "name")` retrieves a specific attribute.
- `hasattr(obj, "name")` checks whether an attribute exists.

Not every field defined by the SDK will necessarily be populated by the API.

## 8\. Extract specific location fields

After inspecting the object, you can access individual attributes directly.

python

```
for location in response.results:
    print("ID:", location.id)
    print("Name:", location.name)
    print("Coordinates:", location.coordinates)
    print("-" * 40)
```

The exact attributes depend on the model returned by your installed SDK.

If an attribute raises an error, inspect the object using `dir(location)` or `vars(location)` to determine the correct field names.

Escritura

## 9\. Explore Sensors

A location may have multiple sensors. Each sensor measures a specific pollutant or environmental parameter.

In the OpenAQ SDK, `Sensor` objects use `__slots__` rather than `__dict__`. Therefore, `vars(sensor)` may raise a `TypeError`.

### 9.1. Print Sensor Information

python

```
for location in response.results:
    print(f"\nLocation: {location.name}")

    for sensor in location.sensors:
        print("Sensor ID:", sensor.id)
        print("Sensor name:", sensor.name)
        print("Parameter:", sensor.parameter)
```

### 9.2. Inspect All Sensor Fields

Use `sensor.__slots__` to retrieve the available field names and `getattr()` to access their values.

python

```
for location in response.results:
    print(f"\nLocation: {location.name}")

    for sensor in location.sensors:
        print("Sensor ID:", sensor.id)
        print("Sensor name:", sensor.name)
        print("Parameter:", sensor.parameter)

        print("All sensor fields:")
        for field in sensor.__slots__:
            print(f"  {field}: {getattr(sensor, field)}")
```

This approach avoids the `TypeError` caused by calling `vars(sensor)` on a slotted object.

### 9.3. Discover Available Sensor Attributes

To inspect all attributes and methods exposed by a sensor:

python

```
if response.results:
    location = response.results[0]

    if location.sensors:
        sensor = location.sensors[0]

        print("Sensor type:", type(sensor))
        print("Available attributes:")
        print(dir(sensor))

        print("Defined slots:")
        print(sensor.__slots__)
```

The attributes exposed by the SDK may include:

- `id`: identifier of the sensor.
- `name`: sensor name, if available.
- `parameter`: the measured pollutant or environmental parameter.
- `load`: a method or attribute exposed by the SDK for loading additional sensor information.

Inspect the actual object to determine which fields are populated and how each attribute behaves.

### 9.4. Inspect the Parameter Object

The `parameter` attribute may contain additional information about the pollutant being measured.

python

```
if response.results:
    location = response.results[0]

    if location.sensors:
        sensor = location.sensors[0]
        parameter = sensor.parameter

        print("Parameter type:", type(parameter))
        print("Parameter value:", parameter)
        print("Parameter attributes:", dir(parameter))
```

This helps identify the parameter's available metadata, such as its name, units, or other properties when provided by the SDK.

### 9.5. Convert a Sensor to a Dictionary

Because `vars(sensor)` may fail, construct a dictionary from the object's slots instead.

python

```
def sensor_to_dict(sensor):
    return {
        field: getattr(sensor, field)
        for field in sensor.__slots__
        if hasattr(sensor, field)
    }
```

Print the sensor as formatted JSON:

python

```
import json

for location in response.results:
    print(f"\nLocation: {location.name}")

    for sensor in location.sensors:
        print(
            json.dumps(
                sensor_to_dict(sensor),
                indent=2,
                default=str
            )
        )
```

**Important:** A sensor describes what is measured; it does not necessarily contain the actual measurement readings. To retrieve air-quality values and timestamps, query the appropriate measurements endpoint using the sensor ID and the parameters supported by your installed SDK version.