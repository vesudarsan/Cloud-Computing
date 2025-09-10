# amqp_consumer.py
import pika
import json

RABBIT_URL = "amqp://user:password@localhost:5672/%2F"
TOPIC_ROUTING_KEY = "drone.123.telemetry"   # depends on mapping; check plugin settings
QUEUE_NAME = "drone_telemetry_queue"

connection = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
channel = connection.channel()

# declare a durable queue
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# bind queue to topic exchange (default topic exchange is amq.topic)
channel.queue_bind(queue=QUEUE_NAME, exchange='amq.topic', routing_key=TOPIC_ROUTING_KEY)

def callback(ch, method, properties, body):
    try:
        payload = json.loads(body)
        print("Received:", payload)
        # process payload...
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Processing error:", e)
        # optionally ch.basic_nack(...)

channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
