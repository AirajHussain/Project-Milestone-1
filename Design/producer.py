# Import Google Cloud Pub/Sub client library
from google.cloud import pubsub_v1

# Import standard libraries
import glob      # Used to find files matching a pattern
import json      # Used to convert data to JSON format
import os        # Used to set environment variables
import csv       # Used to read CSV files

# Find the service account JSON file in the current directory
files = glob.glob("*.json")

# Set the Google credentials environment variable using the JSON file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Google Cloud project ID
project_id = "coral-broker-484817-n5"

# Pub/Sub topic name
topic_name = "labelsTopic"

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Create the fully qualified topic path
topic_path = publisher.topic_path(project_id, topic_name)

# Inform the user where messages are being published
print(f"Publishing messages to {topic_path}...\n")

# Open the CSV file containing label data
with open("Labels.csv", newline="", encoding="utf-8-sig") as f:
    # Read the CSV file as a dictionary (column headers → values)
    reader = csv.DictReader(f)

    # Loop through each row in the CSV file
    for row in reader:
        
        # Convert the CSV row dictionary to a JSON-formatted byte string
        message = json.dumps(row).encode("utf-8")

        # Print the record being published
        print("Producing record:", row)

        # Publish the message to the Pub/Sub topic
        future = publisher.publish(topic_path, message)

        # Wait for the publish operation to complete
        future.result()

# Indicate that publishing is complete
print("\nDone publishing.")
