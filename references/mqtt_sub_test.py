#!/usr/bin/env python3
import time
import json
import logging
import paho.mqtt.client as mqtt
import uuid

# CONFIG — edit these
MQTT_HOST = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_USER = None    # or "user"
MQTT_PASS = None    # or "pass"
TOPIC = "spBv1.0/DumsDroneFleet/DDATA/123456789/Mavlink"
CLIENT_ID = f"test-subscriber-{uuid.uuid4().hex[:8]}"  # unique client id
QOS = 1

# logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mqtt_test")

# callbacks
def on_connect(client, userdata, flags, rc):
    logger.info("on_connect rc=%s flags=%s", rc, flags)
    # subscribe AFTER connect and print subscribe result in on_subscribe
    client.subscribe(TOPIC, qos=QOS)
    logger.info("subscribe() called for %s (qos=%s)", TOPIC, QOS)

def on_subscribe(client, userdata, mid, granted_qos):
    logger.info("on_subscribe mid=%s granted_qos=%s", mid, granted_qos)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8', errors='replace')
    except Exception:
        payload = str(msg.payload)
    logger.info("on_message topic=%s qos=%s len=%d payload=%s", msg.topic, msg.qos, len(msg.payload), payload)

def on_disconnect(client, userdata, rc):
    logger.warning("on_disconnect rc=%s", rc)

def on_log(client, userdata, level, buf):
    # useful internal log (connect/disconnect/suback/etc)
    logger.debug("MQTT LOG: %s", buf)

# build client
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)  # try clean_session=False if you want persisted subscriptions
if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_log = on_log

# connect & loop
logger.info("Connecting to %s:%s as client_id=%s", MQTT_HOST, MQTT_PORT, CLIENT_ID)
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("Stopping")
finally:
    client.loop_stop()
    client.disconnect()
