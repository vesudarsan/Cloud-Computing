#!/usr/bin/env python3
"""
mqtt_to_rabbit.py

Listen to an external MQTT broker and forward messages to a RabbitMQ queue.

Usage: edit the CONFIG below or pass in via environment variables as you prefer.
"""
import threading
import queue
import signal
import sys
import time
import json
import logging
from typing import Optional

import paho.mqtt.client as mqtt
import pika

# ---------- Configuration ----------
CONFIG = {
    # MQTT (external broker)
    "mqtt_host": "broker.hivemq.com",
    "mqtt_port": 1883,
    "mqtt_username": None,    # or "user"
    "mqtt_password": None,    # or "pass"
    "mqtt_topics": [("spBv1.0/DumsDroneFleet/DDATA/123456789/Mavlink", 1)],  # list of (topic, qos)
    "mqtt_client_id": "mqtt-to-rabbit-bridge-1",
    "mqtt_keepalive": 60,
    "mqtt_tls": False,        # set True and provide tls params below if needed
    # RabbitMQ (AMQP)
    "rabbit_url": "amqp://user:password@localhost:5672/%2F",
    "rabbit_queue": "drone_telemetry_queue",
    "rabbit_exchange": "amq.topic",  # default topic exchange; set to '' for direct to queue
    "rabbit_routing_key_template": None,  # if None use mapping function below
    "convert_slash_to_dot": True,   # convert MQTT topic 'a/b/c' -> 'a.b.c' for routing key
    # runtime
    "max_queue_size": 10000,   # inbound queue capacity
    "reconnect_delay": 5,      # seconds to wait before reconnect attempt on rabbit failure
}
# ---------- End configuration ----------

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mqtt_to_rabbit")

# Thread-safe queue for messages received from MQTT
msg_q: queue.Queue = queue.Queue(maxsize=CONFIG["max_queue_size"])
stop_event = threading.Event()

# ---------- Helper: topic -> routing key ----------
def topic_to_routing_key(topic: str) -> str:
    """Map MQTT topic to AMQP routing key. If convert_slash_to_dot True, convert / -> ."""
    if CONFIG["rabbit_routing_key_template"]:
        # you could implement formatting here
        return CONFIG["rabbit_routing_key_template"].format(topic=topic)
    if CONFIG["convert_slash_to_dot"]:
        return topic.replace("/", ".")
    return topic

# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        # Subscribe to topics
        for t, qos in CONFIG["mqtt_topics"]:
            client.subscribe(t, qos=qos)
            logger.info("Subscribed to %s (qos=%s)", t, qos)
    else:
        logger.warning("MQTT connect returned result code %s", rc)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("Unexpected MQTT disconnect (rc=%s). Client will auto-reconnect.", rc)
    else:
        logger.info("MQTT disconnected cleanly (rc=0).")

def on_message(client, userdata, msg: mqtt.MQTTMessage):
    print("Hello World on_message")
    """Called in MQTT network thread. MUST be quick — we only enqueue the message."""
    try:
        payload_bytes = msg.payload  # raw bytes
        # Optionally decode to text or attempt JSON detection here
        # Put a small object in the queue for the Rabbit worker to publish
        item = {
            "topic": msg.topic,
            "payload": payload_bytes,
            "qos": msg.qos,
            "timestamp": time.time(),
            "properties": None,  # reserved
        }
        print(item)
        msg_q.put_nowait(item)
        logger.debug("Enqueued message from topic %s (len=%d)", msg.topic, len(payload_bytes))
    except queue.Full:
        # Queue full -> either drop or implement backpressure
        logger.error("Message queue full; dropping message from topic %s", msg.topic)

# ---------- RabbitMQ publisher worker ----------
class RabbitWorker(threading.Thread):
    def __init__(self, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

    def connect(self):
        """Establish pika BlockingConnection and channel; declare queue."""
        params = pika.URLParameters(CONFIG["rabbit_url"])
        while not self.stop_event.is_set():
            try:
                logger.info("Connecting to RabbitMQ at %s", CONFIG["rabbit_url"])
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                # Declare durable queue
                self.channel.queue_declare(queue=CONFIG["rabbit_queue"], durable=True)
                # If using exchange mapping bind is done outside (consumers), we just publish to exchange
                logger.info("Connected to RabbitMQ and ensured queue %s", CONFIG["rabbit_queue"])
                return
            except Exception as e:
                logger.exception("Failed to connect to RabbitMQ: %s. Retrying in %s sec.", e, CONFIG["reconnect_delay"])
                time.sleep(CONFIG["reconnect_delay"])

    def publish(self, routing_key: str, payload: bytes):
        """Publish payload to RabbitMQ exchange with persistent delivery."""
        if not self.channel or self.channel.is_closed:
            raise RuntimeError("RabbitMQ channel not ready")
        properties = pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/octet-stream"
        )
        # If you want to publish directly to queue without exchange, set exchange=''
        exchange = CONFIG["rabbit_exchange"] or ""
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=payload, properties=properties)
        logger.debug("Published to exchange=%s routing_key=%s len=%d", exchange, routing_key, len(payload))

    def run(self):
        while not self.stop_event.is_set():
            try:
                if not self.connection or self.connection.is_closed:
                    self.connect()
                # Wait for next item (blocking with timeout so we can check stop_event)
                try:
                    item = msg_q.get(timeout=1.0)
                except queue.Empty:
                    continue
                try:
                    routing_key = topic_to_routing_key(item["topic"])
                    self.publish(routing_key, item["payload"])
                    # Optionally you can ack or log; here it's forwarded so nothing to ack
                except Exception:
                    logger.exception("Failed to publish message to RabbitMQ; re-enqueueing and reconnecting")
                    # If publish failed, requeue item (or drop depending on policy) and reconnect
                    try:
                        msg_q.put_nowait(item)
                    except queue.Full:
                        logger.error("Requeue failed, queue full — dropping message")
                    # Force reconnect
                    try:
                        if self.connection:
                            self.connection.close()
                    except Exception:
                        pass
                    time.sleep(CONFIG["reconnect_delay"])
                finally:
                    msg_q.task_done()
            except Exception:
                logger.exception("Unexpected error in RabbitWorker loop")
        # cleanup
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception:
            pass
        logger.info("RabbitWorker exiting")

# ---------- Main bootstrap ----------
def main():
    # Setup MQTT client
    mqtt_client = mqtt.Client(client_id=CONFIG["mqtt_client_id"], clean_session=True)
    if CONFIG["mqtt_username"]:
        mqtt_client.username_pw_set(CONFIG["mqtt_username"], CONFIG["mqtt_password"])
    if CONFIG["mqtt_tls"]:
        # Example TLS config; customize with ca_certs, certfile, keyfile as needed
        mqtt_client.tls_set()  # uses system CA certs — for custom CA provide ca_certs path
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    # Start Rabbit worker thread
    worker = RabbitWorker(stop_event)
    worker.start()

    # Handle signals for graceful shutdown
    def _signal_handler(sig, frame):
        logger.info("Received signal %s: shutting down...", sig)
        stop_event.set()
        try:
            mqtt_client.disconnect()
        except Exception:
            pass

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Connect MQTT and start loop in background thread
    logger.info("Connecting to MQTT broker %s:%s", CONFIG["mqtt_host"], CONFIG["mqtt_port"])
    try:
        mqtt_client.connect(CONFIG["mqtt_host"], CONFIG["mqtt_port"], keepalive=CONFIG["mqtt_keepalive"])
    except Exception:
        logger.exception("Initial MQTT connect failed; will rely on client internal reconnect")
    mqtt_client.loop_start()

    # Keep main thread alive until stop_event set
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        logger.info("Main loop exiting; waiting for worker to finish")
        worker.join(timeout=5.0)
        mqtt_client.loop_stop()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    main()
