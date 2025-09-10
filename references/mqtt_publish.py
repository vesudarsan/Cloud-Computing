# mqtt_publish.py
import paho.mqtt.client as mqtt
import json
import time

MQTT_HOST = "localhost"
MQTT_PORT = 1883
TOPIC = "drone/123/telemetry"

client = mqtt.Client(client_id="drone-123")
client.username_pw_set("user", "password")
client.connect(MQTT_HOST, MQTT_PORT, 60)

payload = {"lat": 12.34, "lon": 56.78, "battery": 87}

client.publish(TOPIC, json.dumps(payload), qos=1)  # use qos 1 for reliability
time.sleep(0.2)
client.disconnect()
