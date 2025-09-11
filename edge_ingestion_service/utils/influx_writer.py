# influx_writer.py

from influxdb_client import InfluxDBClient, Point,WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from utils.logger import setup_logger
logging = setup_logger(__name__)


class InfluxWriter:
    def __init__(self, url, token, org, bucket):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket
        self.org = org

    def write_points(self, payload,droneId,message_type):
        try:

            timestamp = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))

            # Use messageType as measurement name
            message_type = payload.get("messageType", "unknown")

            # Create point with measurement and drone_id tag
            point = Point(message_type).tag("drone_id", droneId).tag("messageType", message_type)

            # Add all numeric fields dynamically
            for key, value in payload.items():
                if key not in ["timestamp", "messageType"] and isinstance(value, (int, float)):
                    point.field(key, value)

            # Set timestamp
            point.time(timestamp)

            # Write to InfluxDB
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            logging.debug(f"📤 Written to InfluxDB: {message_type} data for drone {droneId}")
 
        except Exception as e:
            logging.error(f"❌ Failed to write to InfluxDB: {e}")

    def close(self):
        self.client.close()
        logging.info("✅ InfluxDB disconnected")