import csv
import signal
import sys
import os
import time
import uvicorn
import json

import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.responses import FileResponse
from datetime import datetime

app = FastAPI()

csv_file_path = "mqtt_data.csv"

# MQTT settings
mqtt_broker_host = "broker.hivemq.com"  # Replace with the hostname or IP address of the MQTT broker
mqtt_topic = "Brainsoft-Raxat-ESP32/pub"  # Replace with the topic you want to subscribe to

def create_csv_file():
    # Check if the CSV file exists
    if not os.path.isfile(csv_file_path):
        # Create the CSV file and write the header row
        with open(csv_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["created_at", "lux", "current", "power"])

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe to the topic
        client.subscribe(mqtt_topic)
    else:
        print("Failed to connect, return code: ", rc)

def on_message(client, userdata, msg):
    print(f"Received message on topic: {msg.topic}")
    print(f"Message: {msg.payload}")

    # Write the received data to the CSV file
    with open(csv_file_path, "a") as csvfile:
        writer = csv.writer(csvfile)
        current_time_utc = datetime.utcnow()
        time_string_utc = current_time_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        data = json.loads(msg.payload.decode('utf-8'))

        writer.writerow([time_string_utc, data['lux'], data['current'], data['power']])


def mqtt_subscriber():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the MQTT broker
    client.connect(mqtt_broker_host, 1883, 60)

    # Subscribe to the MQTT topic
    client.subscribe(mqtt_topic)

    try:
        # Loop to process MQTT messages
        client.loop_start()
        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        # Disconnect from MQTT broker on program exit
        client.loop_stop()
        client.disconnect()


@app.on_event("startup")
async def startup_event():
    create_csv_file()

@app.get("/csv")
async def get_csv_file():
    return FileResponse(csv_file_path)


if __name__ == "__main__":
    # Start the MQTT subscriber in a separate thread
    import threading

    mqtt_thread = threading.Thread(target=mqtt_subscriber)
    mqtt_thread.start()

    # Start the FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=8000)

    # Wait for the MQTT thread to finish
    mqtt_thread.join()
