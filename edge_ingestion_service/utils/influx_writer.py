# influx_writer.py

from influxdb_client import InfluxDBClient, Point,WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import warnings
from influxdb_client.client.warnings import MissingPivotFunction
from utils.logger import setup_logger
import json
logging = setup_logger(__name__)

warnings.simplefilter("ignore", MissingPivotFunction)
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

    def write_landing_event(self, drone_id: str, ts: datetime):
        """Write one landing event: measurement=flight_events, field=landings=1"""
        try:
            p = (
                Point("flight_events")
                .tag("drone_id", drone_id)
                .tag("event", "landing")
                .field("landings", 1)
                .time(ts, WritePrecision.NS)
            )
            self.write_api.write(bucket=self.bucket, org=self.org, record=p)
            logging.debug(f"📤 Landing event written for {drone_id} at {ts.isoformat()}")
        except Exception as e:
            logging.error(f"❌ Failed to write landing event: {e}")


    def write_alarm_event(self, drone_id: str, ts, event: str, severity: str, meta: dict | None = None):
        try:
            p = (Point("flight_alarms")
                .tag("drone_id", drone_id)
                .tag("event", event)           # "hard_landing" | "crash"
                .tag("severity", severity)     # "warn" | "critical"
                .field("count", 1))
            if meta:
                for k, v in meta.items():
                    # store numeric meta as fields; stringify others
                    if isinstance(v, (int, float)):
                        p = p.field(k, v)
                    else:
                        p = p.tag(k, str(v))
            p = p.time(ts, WritePrecision.NS)
            self.write_api.write(bucket=self.bucket, org=self.org, record=p)
            logging.debug(f"📤 Alarm event written for {drone_id}: {event}/{severity}")
        except Exception as e:
            logging.error(f"❌ Failed to write alarm event: {e}")

    def write_device_state(self, drone_id: str, online: bool, ts=None):
            """
            Write device ONLINE/OFFLINE event.

            measurement: device_state
            tags: drone_id
            field: online (1 or 0)
            time: ts (datetime) or now
            """
            try:
                if ts is None:
                    from datetime import datetime, timezone
                    ts = datetime.now(timezone.utc)
                p = (
                    Point("device_state")
                    .tag("drone_id", str(drone_id))
                    .field("online", 1 if online else 0)
                    .time(ts, WritePrecision.NS)
                )
                self.write_api.write(bucket=self.bucket, org=self.org, record=p)
                logging.debug(f"📤 device_state written for {drone_id}: online={1 if online else 0}")
            except Exception as e:
                logging.exception(f"❌ Failed to write device_state for {drone_id}: {e}")


    def write_device_birth(self, payload: dict, drone_id: str):
            """
            Write NBIRTH payload into measurement `device_birth`.
            Stores system.* fields as numeric fields and deployments as JSON string.
            """
            try:
                # timestamp fallback: payload.start_time or now()
                ts_str = payload.get("start_time")
                if ts_str:
                    timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                else:
                    timestamp = datetime.now(timezone.utc)

                p = Point("device_birth").tag("drone_id", drone_id).tag("status", payload.get("status", "online"))

                system = payload.get("system", {})
                # numeric system fields (if present)
                if "hostname" in system:
                    p.tag("hostname", system.get("hostname"))
                if "ip_address" in system:
                    p.tag("ip_address", system.get("ip_address"))
                if "uptime_seconds" in system:
                    try:
                        p.field("uptime_seconds", float(system.get("uptime_seconds") or 0.0))
                    except:
                        pass
                if "cpu_percent" in system:
                    try:
                        p.field("cpu_percent", float(system.get("cpu_percent") or 0.0))
                    except:
                        pass
                mem = system.get("memory") or {}
                if "total_mb" in mem:
                    try:
                        p.field("mem_total_mb", float(mem.get("total_mb") or 0.0))
                    except:
                        pass
                if "used_mb" in mem:
                    try:
                        p.field("mem_used_mb", float(mem.get("used_mb") or 0.0))
                    except:
                        pass
                if "percent" in mem:
                    try:
                        p.field("mem_percent", float(mem.get("percent") or 0.0))
                    except:
                        pass

                # deployments: store as JSON string field (and optionally count)
                deployments = payload.get("deployments")
                if deployments is not None:
                    try:
                        p.field("deployments_json", json.dumps(deployments))
                        p.field("deployments_count", len(deployments))
                    except Exception:
                        # fallback: store str()
                        p.field("deployments_json", str(deployments))

                p.time(timestamp)  # use provided start_time as point time

                self.write_api.write(bucket=self.bucket, org=self.org, record=p)
                logging.debug(f"📤 Written device_birth for {drone_id}")
            except Exception as e:
                logging.error(f"❌ Failed to write device_birth: {e}")

    def write_device_death(self, payload: dict, drone_id: str):
        """
        Write NDEATH event into measurement `device_death`.
        """
        try:
            # timestamp fallback to now
            ts = payload.get("start_time") or payload.get("time") or None
            if ts:
                try:
                    timestamp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except:
                    timestamp = datetime.now(timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)

            p = Point("device_death").tag("drone_id", drone_id).tag("status", payload.get("status", "offline"))
            # write a few optional fields
            if "reason" in payload:
                p.field("reason", str(payload.get("reason")))

            p.time(timestamp)
            self.write_api.write(bucket=self.bucket, org=self.org, record=p)
            logging.debug(f"📤 Written device_death for {drone_id}")
        except Exception as e:
            logging.error(f"❌ Failed to write device_death: {e}")

    def close(self):
        self.client.close()
        logging.info("✅ InfluxDB disconnected")