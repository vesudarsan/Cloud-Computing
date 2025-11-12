import paho.mqtt.client as mqtt
import json
import time
import psutil
import socket
import platform
from utils.logger import setup_logger
from utils.influx_writer import InfluxWriter
import math, os, threading, subprocess
from datetime import datetime, timezone, timedelta
from utils.rest_client import RestClient
from collections import defaultdict,deque

logging = setup_logger(__name__)

# ---- Tunables via env (sane defaults) ----
ALT_START = float(os.getenv("FS_ALT_START", "1.5"))     # m  -> start if >=
ALT_END   = float(os.getenv("FS_ALT_END",   "0.8"))     # m  -> end when <=
SPD_START = float(os.getenv("FS_SPD_START", "0.8"))     # m/s
SPD_END   = float(os.getenv("FS_SPD_END",   "0.3"))     # m/s
START_HOLD = int(os.getenv("FS_START_HOLD", "3"))       # samples to confirm takeoff
END_HOLD   = int(os.getenv("FS_END_HOLD",   "3"))       # samples to confirm landing
MIN_FLIGHT_SEC = int(os.getenv("FS_MIN_FLIGHT_SEC", "5"))
PAD_SEC        = int(os.getenv("FS_PAD_SEC", "2"))      # pad window on both ends
MAX_CLOCK_SKEW_SEC = 5

def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00","Z")

# InfluxDB Config
INFLUX_TOKEN = "drone-secret-token"
INFLUX_ORG = "drone-org"
INFLUX_BUCKET = "drone_telemetry"

if platform.system() == "Windows":  # 2dl read from config
    INFLUX_URL = "http://localhost:8086"
else:
    INFLUX_URL = "http://host.docker.internal:8086"

class MQTTClient:
    def __init__(self, broker, port, drone_id, sparkplug_namespace,
                 sp_group_id, sp_edge_id, sp_device_id):
        self.client = mqtt.Client(client_id=str(sp_device_id), clean_session=True)
        self.broker = broker
        self.port = port
        self.connected = False

        self.drone_id = drone_id
        self.sparkplug_namespace = sparkplug_namespace
        self.sp_group_id = sp_group_id
        self.sp_edge_id = sp_edge_id
        self.sp_device_id = sp_device_id
        self.drone_location = {'lat': 0.0, 'lon': 0.0}
        self.TOPIC_PREFIX = f"{sparkplug_namespace}/{sp_group_id}/NCMD/{sp_edge_id}"
        self.rest_client = RestClient()
        self.influx_writer = InfluxWriter(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            bucket=INFLUX_BUCKET
        )

        # ---- Multi-drone flight & clock state ----
        self._fs = {}            # {droneId: {...}}
        self._clk = {}           # {droneId: {"unix": dt, "boot_ms": int}}
        # Compute orchestration
        self._compute_lock = threading.Lock()     # optional global lock (kept for safety)
        self._inflight_by_drone = defaultdict(bool)  # {droneId: bool}
        self._last_window = {}                       # {droneId: (start_iso, end_iso)}

        self._buf = {}  # {drone_id: deque of samples (ts, alt_m, v_h, v_vert)}
        self._imu = {}  # {drone_id: last accel magnitude, g}
        self._buf_len_sec = float(os.getenv("CRASH_BUF_SEC", "3.0"))
        self.online_devices = {}   # drone_id -> {"last_seen": iso, "payload": {...}}

    ## Code for buffering and impact detection , used to detect impacts like crashes and hard landings
    def _buf_get(self, drone_id):
        q = self._buf.get(drone_id)
        if q is None:
            q = deque()
            self._buf[drone_id] = q
        return q

    def _push_sample(self, drone_id, ts, rel_alt_mm, vx_cms, vy_cms):
        q = self._buf_get(drone_id)
        alt_m = (rel_alt_mm or 0)/1000.0
        v_h = ( ((vx_cms or 0)**2 + (vy_cms or 0)**2) ** 0.5 ) / 100.0  # m/s
        # estimate vertical speed from alt derivative using last sample
        v_vert = 0.0
        if q:
            (ts_prev, alt_prev, _, _) = q[-1]
            dt = (ts - ts_prev).total_seconds()
            if dt > 0:
                v_vert = (alt_m - alt_prev) / dt
        q.append((ts, alt_m, v_h, v_vert))
        # trim older than buffer window
        while q and (ts - q[0][0]).total_seconds() > self._buf_len_sec:
            q.popleft()

    def _update_imu(self, drone_id, xacc, yacc, zacc):
        # if you have HIGHRES_IMU (m/s^2), store magnitude in g
        g = ( (xacc or 0)**2 + (yacc or 0)**2 + (zacc or 0)**2 ) ** 0.5 / 9.80665
        self._imu[drone_id] = g

    def _detect_impact(self, drone_id):
        q = self._buf_get(drone_id)
        if len(q) < 3:
            return None  # not enough data

        # Look at the last ~1 s window
        ts_end = q[-1][0]
        recent = [s for s in q if (ts_end - s[0]).total_seconds() <= 1.2]
        if len(recent) < 3:
            return None

        # pre-touch metrics (max before last)
        pre = recent[:-1]
        v_h_pre   = max(p[2] for p in pre)
        v_vert_dn = min(p[3] for p in pre)  # negative when descending

        # post-touch metrics (last point)
        v_h_last  = recent[-1][2]
        alt_last  = recent[-1][1]
        v_vert_last = recent[-1][3]

        # thresholds (tunable via env)
        VDN_HARD = float(os.getenv("CRASH_VDN_HARD", "2.5"))   # m/s downwards
        VH_HARD  = float(os.getenv("CRASH_VH_HARD",  "2.0"))   # m/s horiz
        VDN_CRIT = float(os.getenv("CRASH_VDN_CRIT", "4.0"))
        VH_ZERO  = float(os.getenv("CRASH_VH_ZERO",  "0.5"))   # ~stopped
        ALT_END  = float(os.getenv("FS_ALT_END", "0.8"))
        G_IMPACT = float(os.getenv("CRASH_G_IMPACT", "15.0"))  # 15g

        # IMU spike (if present)
        g_peak = self._imu.get(drone_id, 0.0)

        hard = (v_vert_dn <= -VDN_HARD and v_h_pre >= VH_HARD and v_h_last <= VH_ZERO and alt_last <= ALT_END)
        crit = (g_peak >= G_IMPACT) or (v_vert_dn <= -VDN_CRIT and v_h_last <= VH_ZERO and alt_last <= ALT_END)

        if crit:
            return ("crash", "critical", {
                "v_down": round(-v_vert_dn, 2), "v_h_pre": round(v_h_pre, 2), "g": round(g_peak,1)
            })
        if hard:
            return ("hard_landing", "warn", {
                "v_down": round(-v_vert_dn, 2), "v_h_pre": round(v_h_pre, 2), "g": round(g_peak,1)
            })
        return None

    def connect(self, topic, lwt_message, qos=1, retain=True):
        print("self.sp_device_id:",self.sp_device_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.will_set(topic, lwt_message, qos, retain)
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def disconnect(self):
        disconnect_msg = json.dumps({
            "drone_id": self.drone_id,
            "status": "disconnect",
            "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })
        
        topic = f"{self.sparkplug_namespace}/{self.sp_group_id}/NDEATH/{self.sp_device_id}"
        self.client.publish(topic, payload=disconnect_msg, qos=1, retain=False)
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.influx_writer.close()
        finally:
            self.connected = False
            logging.info("✅ MQTT disconnected")

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)
        if self.connected:
            logging.info(f"✅ Connected to MQTT broker [{self.broker}:{self.port}] with code {rc}")
        else:
            logging.error(f"❌ Failed to connect to MQTT broker [{self.broker}:{self.port}] with code {rc}")
            return

        topics = [
            # f"{self.sparkplug_namespace}/{self.sp_group_id}/NBIRTH/{self.sp_edge_id}",
            # f"{self.sparkplug_namespace}/{self.sp_group_id}/NDEATH/{self.sp_edge_id}",
            # f"{self.sparkplug_namespace}/{self.sp_group_id}/DDATA/{self.sp_edge_id}/Mavlink"
            f"{self.sparkplug_namespace}/{self.sp_group_id}/NBIRTH/+",
            f"{self.sparkplug_namespace}/{self.sp_group_id}/NDEATH/+",
            f"{self.sparkplug_namespace}/{self.sp_group_id}/DDATA/+/Mavlink"
        ]
        for t in topics:
            try:
                client.subscribe(t)
                logging.info(f"📡 Subscribed to topic: {t}")
            except Exception as e:
                logging.error(f"⚠️ Failed to subscribe to {t}: {e}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        logging.warning("❌ MQTT disconnected")

    def subscribe(self, topic):
        self.client.client.subscribe(topic)
        self.client.client.on_message = self._on_message

    def update_drone_location(self, droneId, payload):
        self.drone_location['droneId'] = droneId
        self.drone_location['lat'] = payload['lat']
        self.drone_location['lon'] = payload['lon']
        self.drone_location['alt'] = payload['alt']
        self.drone_location['heading'] = payload['hdg']

    def get_drone_location(self):
        return self.drone_location

    def _on_message(self, client, userdata, msg):
        try:
            logging.debug(f"📩 Received message on {msg.topic}: {msg.payload.decode('utf-8')}")
            topic = msg.topic
            payload = json.loads(msg.payload.decode())
            if "NBIRTH" in topic or "NDEATH" in topic:
                droneId = topic.split("/")[-1]
            else:
                droneId = topic.split("/")[-2]          


            # detect NBIRTH/NDEATH from topic (example topics):
            # spBv1.0/DumsDroneFleet/NBIRTH/123456789
            # spBv1.0/DumsDroneFleet/NDEATH/123456789
   
            # Better safe: check for "NBIRTH" / "NDEATH" anywhere in topic
            if "NBIRTH" in topic:
                # device online
                try:
                    # write to Influx
                    self.influx_writer.write_device_state(drone_id=droneId, online=True)                    
                except Exception:
                    logging.exception(f"[{droneId}] failed processing NBIRTH")

                try:
                    self.influx_writer.write_device_birth(payload, droneId)
                except Exception as e:
                    logging.error(f"Failed writing NBIRTH to Influx: {e}") 

                # update in-memory online devices map
                self.online_devices[droneId] = {
                    "drone_id": droneId,
                    "last_seen": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                    "payload": payload
                }
                logging.info(f"[{droneId}] NBIRTH processed, marked online")                   

            elif "NDEATH" in topic:
                # device offline
                try:
                    self.influx_writer.write_device_state(drone_id=droneId, online=False)                    
                   
                except Exception:
                    logging.exception(f"[{droneId}] failed processing NDEATH")   

                try:
                    self.influx_writer.write_device_death(payload, droneId)
                except Exception as e:
                    logging.error(f"Failed writing NDEATH to Influx: {e}")

                # remove from online map (but keep a last_seen field if desired)
                prev = self.online_devices.pop(droneId, None)
                logging.info(f"[{droneId}] NDEATH processed, removed from online list")                 

            
            elif "Mavlink" in topic:
            # if topic.endswith("/Mavlink"):
                message_type = None
                payload = json.loads(msg.payload.decode())
                message_type = payload.get("messageType")
                # write to Influx
                self.influx_writer.write_points(payload, droneId, message_type)

                # SYSTEM_TIME anchor
                if message_type == "SYSTEM_TIME":
                    tus = payload.get("time_unix_usec")
                    tbm = payload.get("time_boot_ms")
                    if tus is not None and tbm is not None:
                        self._clock_update(droneId, int(tus), int(tbm))

                # GLOBAL_POSITION_INT -> flight state tracker
                if message_type == "GLOBAL_POSITION_INT":
                    self.update_drone_location(droneId, payload)
                    self._on_gpos_for_flight_state(droneId, payload)

        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in payload: {e}")
        except Exception as e:
            logging.exception(f"Error processing message: {e}")

    def publish(self, topic=None, payload=None, qos=1, storeAndForward=False):
        actual_topic = topic or self.topic
        if not self.connected:
            logging.warning("❌ MQTT not connected")
            return None
        logging.info(f"✅ Published to {actual_topic} [qos={qos}]")
        return self.client.publish(actual_topic, payload, qos=qos)

    def is_connected(self):
        return self.connected

    # --------- CLOCK / STATE UTILS ----------
    def _clock_update(self, drone_id: str, time_unix_usec: int, time_boot_ms: int):
        """Anchor boot_ms to UTC using SYSTEM_TIME."""
        self._clk[drone_id] = {
            "unix": datetime.fromtimestamp(time_unix_usec/1e6, tz=timezone.utc),
            "boot_ms": int(time_boot_ms)
        }

    def _utc_from_boot(self, drone_id: str, time_boot_ms: int) -> datetime:
        """Translate boot_ms to UTC using last SYSTEM_TIME; fallback to now()."""
        anchor = self._clk.get(drone_id)
        if anchor and time_boot_ms is not None:
            delta_ms = int(time_boot_ms) - anchor["boot_ms"]
            return anchor["unix"] + timedelta(milliseconds=delta_ms)
        return datetime.now(timezone.utc)

    def _fs_get(self, drone_id: str):
        st = self._fs.get(drone_id)
        if st is None:
            st = {
                "in_air": False,
                "hold_start": 0,
                "hold_end": 0,
                "flight_start": None,
                "last_ts": None,
            }
            self._fs[drone_id] = st
        return st

    def _prune_idle_drone(self, drone_id: str, idle_sec: int = 900):
        """Drop per-drone state after being idle on ground for a while."""
        st = self._fs.get(drone_id)
        if not st or st.get("in_air"): 
            return
        last = st.get("last_ts")
        if last and (datetime.now(timezone.utc) - last).total_seconds() > idle_sec:
            self._fs.pop(drone_id, None)
            self._clk.pop(drone_id, None)
            # keep _last_window so we don't immediately re-run on reconnect; remove if you prefer

    # --------- COMPUTE TRIGGER ----------
    def _trigger_compute_async(self, drone_id: str, start_utc: datetime, end_utc: datetime):
        """
        Run compute script in a background thread, one per drone at a time,
        with an isolated environment (safe for multiple drones).
        """
        start_iso = _iso(start_utc)
        end_iso   = _iso(end_utc)

        # de-dup exact same window
        last = self._last_window.get(drone_id)
        if last == (start_iso, end_iso):
            logging.info(f"[{drone_id}] Skipping duplicate compute for window {start_iso} → {end_iso}")
            return

        if self._inflight_by_drone[drone_id]:
            logging.info(f"[{drone_id}] Compute already in-flight; will not start another.")
            return

        def _run():
            self._inflight_by_drone[drone_id] = True
            try:
                env = os.environ.copy()
                env["DRONE_VALUE"]  = str(drone_id)
                env["WINDOW_START"] = start_iso
                env["WINDOW_END"]   = end_iso

                logging.info(f"[{drone_id}] Running flight-hours for window {start_iso} → {end_iso}")
                # Run as module so imports resolve inside package
                # Requires core/__init__.py and running from project root
                subprocess.run(
                    [os.sys.executable, "-m", "core.compute_flight_hours_from_gps"],
                    env=env, check=True
                )
                self._last_window[drone_id] = (start_iso, end_iso)
                logging.info(f"[{drone_id}] Flight-hours compute finished.")
            except subprocess.CalledProcessError as e:
                logging.exception(f"[{drone_id}] flight-hours compute failed (exit={e.returncode}): {e}")
            except Exception as e:
                logging.exception(f"[{drone_id}] flight-hours compute error: {e}")
            finally:
                self._inflight_by_drone[drone_id] = False

        threading.Thread(target=_run, daemon=True).start()

    # --------- FLIGHT STATE from GPOS ----------
    def _on_gpos_for_flight_state(self, drone_id: str, payload: dict):
        """
        Derive IN_AIR/landed from GLOBAL_POSITION_INT (relative_alt mm, vx/vy cm/s).
        Trigger compute on landing.
        """
        st = self._fs_get(drone_id)

        rel_alt_mm   = payload.get("relative_alt")
        vx_cms       = payload.get("vx")
        vy_cms       = payload.get("vy")
        time_boot_ms = payload.get("time_boot_ms")

        # Timestamp preference: convert boot time to UTC using last SYSTEM_TIME; else now()
        ts = self._utc_from_boot(drone_id, time_boot_ms) if time_boot_ms is not None else datetime.now(timezone.utc)

        # Convert units
        alt_m  = (rel_alt_mm or 0)/1000.0
        spd_ms = math.sqrt((vx_cms or 0)**2 + (vy_cms or 0)**2)/100.0

        # --- NEW: Feed rolling buffer & check crash/hard landing ---
        self._push_sample(drone_id, ts, rel_alt_mm, vx_cms, vy_cms)
        impact = self._detect_impact(drone_id)
        if impact:
            event, severity, meta = impact
            logging.warning(f"[{drone_id}] Impact detected: {event} ({severity}) {meta}")
            self.influx_writer.write_alarm_event(
                drone_id=drone_id, ts=ts, event=event, severity=severity, meta=meta
        )

        above_start = (alt_m >= ALT_START) or (spd_ms >= SPD_START)
        below_end   = (alt_m <= ALT_END)   and (spd_ms <= SPD_END)

        st["last_ts"] = ts

        # TAKEOFF detection (debounced)
        if not st["in_air"]:
            if above_start:
                st["hold_start"] += 1
                if st["hold_start"] >= START_HOLD:
                    st["in_air"] = True
                    st["flight_start"] = ts
                    st["hold_start"] = 0
                    st["hold_end"] = 0
                    logging.info(f"[{drone_id}] TAKEOFF at { _iso(ts) } alt={alt_m:.2f}m v={spd_ms:.2f}m/s")
            else:
                st["hold_start"] = 0
            return

        # LANDING detection (debounced)
        if st["in_air"]:
            if below_end:
                st["hold_end"] += 1
                if st["hold_end"] >= END_HOLD:
                    st["in_air"] = False
                    start = st["flight_start"]
                    end   = ts
                    st["hold_end"] = 0
                    st["hold_start"] = 0
                    st["flight_start"] = None

                    if start:
                        dur = (end - start).total_seconds()
                        if dur >= MIN_FLIGHT_SEC:
                            start_p = start - timedelta(seconds=PAD_SEC)
                            end_p   = end + timedelta(seconds=PAD_SEC)
                            if (end_p - start_p).total_seconds() > (dur + 2*PAD_SEC + MAX_CLOCK_SKEW_SEC):
                                logging.warning(f"[{drone_id}] large clock skew detected on landing window.")
                            logging.info(f"[{drone_id}] LANDED at { _iso(ts) } — duration {dur:.1f}s → compute")
                            self.influx_writer.write_landing_event(drone_id=drone_id, ts=ts) 
                            self._trigger_compute_async(drone_id, start_p, end_p)
                        else:
                            logging.info(f"[{drone_id}] landed but short flight ignored ({dur:.1f}s)")
            else:
                st["hold_end"] = 0

        # Optional: prune idle on-ground drones to free memory
        self._prune_idle_drone(drone_id)
