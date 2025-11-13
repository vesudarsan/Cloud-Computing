from flask import Flask, request, jsonify,render_template,send_from_directory
from utils.logger import setup_logger
from flask_cors import CORS
import os
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
from influxdb_client import InfluxDBClient
import platform

logging = setup_logger(__name__)

result = []

if platform.system() == "Windows":  # 2dl read from config
    INFLUX_URL = "http://localhost:8086"
else:
    INFLUX_URL = "http://host.docker.internal:8086"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "UGFYphuRbH-x-KTWrHfiQNbNSH2Wg1R9KPHkp0Ga8WRlbkGTMB2-w2-e8J9xKDMQgotVdhLEHVr82Ll9W7VXvw==")  # avoid committing this
INFLUX_ORG   = os.getenv("INFLUX_ORG", "5ea549634c8f37ac")

BUCKET       = os.getenv("OUT_BUCKET",   os.getenv("IN_BUCKET", "drone_telemetry"))
MEAS_SUMMARY = os.getenv("MEAS_SUMMARY", "flight_hours")
DRONE_TAG    = os.getenv("DRONE_TAG",    "drone_id")

_influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
_qapi = _influx_client.query_api()


def try_parse_json(s):
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return s

def try_parse_number(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        try:
            return float(str(v))
        except Exception:
            return None

def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00","Z")

def _flux_sum_hours(drone_id: str, start_iso: str, end_iso: str) -> float:
    q = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start_iso}, stop: {end_iso})
  |> filter(fn: (r) => r._measurement == "{MEAS_SUMMARY}")
  |> filter(fn: (r) => r._field == "hours" and r.{DRONE_TAG} == "{drone_id}")
  |> drop(columns: ["window_start","window_end"])
  |> group(columns: [])
  |> sum()
'''
    df = _qapi.query_data_frame(q)
    if isinstance(df, list) and df:
        df = pd.concat(df, ignore_index=True)
    if df is None or df.empty:
        return 0.0
    try:
        return float(df["_value"].iloc[0])
    except Exception:
        return 0.0
    
def _flux_sum_landings(drone_id: str, start_iso: str, end_iso: str) -> int:
    q = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start_iso}, stop: {end_iso})
  |> filter(fn: (r) => r._measurement == "flight_events")
  |> filter(fn: (r) => r._field == "landings" and r.drone_id == "{drone_id}")
  |> group(columns: [])
  |> sum()
'''
    df = _qapi.query_data_frame(q)
    if isinstance(df, list) and df:
        df = pd.concat(df, ignore_index=True)
    if df is None or df.empty:
        return 0
    try:
        # _value is the sum of 1-per-landing
        return int(float(df["_value"].iloc[0]))
    except Exception:
        return 0

def _flux_online_devices(ttl_seconds: int = 900):
    """
    Query InfluxDB for latest device_state per drone and return those
    whose last state is online==1 *and* the last timestamp is within ttl_seconds.
    """
    # window must cover recent history (we'll pick ttl_seconds or a bit more)
    start_iso = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()  # broad range
    q = f'''
from(bucket: "{BUCKET}")
|> range(start: -7d)
|> filter(fn: (r) => r._measurement == "device_state")
|> drop(columns: [])
|> group(columns: ["drone_id"])
|> last()
|> filter(fn: (r) => r._field == "online")
'''
    df = _qapi.query_data_frame(q)
    if isinstance(df, list) and df:
        df = pd.concat(df, ignore_index=True)

    results = []
    if df is None or df.empty:
        return results

    now = datetime.now(timezone.utc)
    for _, row in df.iterrows():
        try:
            drone_id = row.get("drone_id") or row.get("droneId") or row.get("_measurement")
            val = float(row.get("_value", 0))
            ts = row.get("_time")
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            # TTL check
            # if val == 1 and (now - ts).total_seconds() <= ttl_seconds:
            if val == 1 :
                results.append({"drone_id": str(drone_id), "last_seen": _iso(ts)})
        except Exception:
            continue
    return results


#STATIC_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../static"))

# ------------------------
# Flask Routes
# ------------------------
def register_routes(app,publisher):
    CORS(app)   # 👈 enables CORS for all routes
    @app.get("/check") #2dl check again
    def root():
        return jsonify({"status": "ok"})

    @app.post("/start")
    def start():
        if publisher.running:
            return jsonify({"status": "already running"}), 400
        success = publisher.start()
        return jsonify({"status": "started" if success else "failed"}), 200 if success else 500

   
    @app.post("/stop")
    def stop():
        if publisher.stop():
            return jsonify({"status": "stopped"}), 200
        return jsonify({"status": "not running"}), 400

   
    @app.get("/status")
    def status():
        return jsonify({"running": publisher.running, "mqtt_connected": publisher.mqtt_connected})

    @app.get("/uiStatus")
    def newStatus():
        if publisher.running:
            return jsonify({"status": "running"})
        else:
            return jsonify({"status": "stopped"})

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

      
    @app.post("/publish")
    def publish_message():
        data = request.get_json()
        
        if not data or 'message' not in data:
            return jsonify({"error": "Missing 'message' in request body"}), 400

        topic = data.get('topic', publisher.topic)  # optional override
        message = data['message']

        if publisher.is_mqtt_connected():
            result = publisher.client.publish(topic,message)
         
            if result and getattr(result, "rc", 1) == 0:   
                logging.info(f"Payload: {message}, topic: {topic}")
                
                return jsonify({"status": "published", "topic": topic}), 200
            else:
                # MQTT connected but publish failed — buffer it                
               
                #publisher.store_payload(data)
                #return jsonify({"status": "publish failed, buffered", "topic": topic}), 202
                return jsonify({"status": "publish failed, ", "topic": topic}), 202
        else:
            # MQTT not connected — buffer it
            #publisher.store_payload(data) # 2dl not sure if requried
            None

            #return jsonify({"status": "mqtt disconnected, buffered", "topic": topic}), 202
            return jsonify({"status": "publish failed, ", "topic": topic}), 202
        

    @app.route('/')
    def index():
        return render_template('map.html')        
    # @app.get("/publishNbirth") # 2dl check later if required 
    # def publish_nbirth():
        
    #     publisher.sendNbirthMsg()
    #     return jsonify({"status": "Published MQTT birth message" }), 200

    @app.route('/location')
    def location():
        drone = publisher.client.get_drone_location() 
      
        result = []

        
        result.append({
                "id": drone['droneId'],  # use dict key if no droneId inside
                "lat": drone['lat'] / 1e7,
                "lon": drone['lon'] / 1e7,
                "alt": drone.get('alt', 0),
                "heading": drone.get('heading', 0)
            })
     
        # for drone_id, drone in drone_locations.items():
        #     if drone.get('lat', 0) == 0 or drone.get('lon', 0) == 0:
        #         continue

            # result.append({
            #     'id': drone.get('droneId', drone_id),  # use dict key if no droneId inside
            #     'lat': drone['lat'] / 1e7,
            #     'lon': drone['lon'] / 1e7,
            #     'alt': drone.get('alt', 0),
            #     'heading': drone.get('heading', 0)
            # })

        return jsonify(result)

        
      
        # if drone_location["lat"] == 0 or drone_location["lon"] == 0:
        #     return jsonify({'lat': 0, 'lon': 0})
        # return jsonify({
        #     'lat': drone_location["lat"] / 1e7,
        #     'lon': drone_location["lon"] / 1e7,
        #     'alt': drone_location['alt'],
        #     'heading': drone_location['heading'],
        # })


        # drone_locations = publisher.client.get_drone_location()  # Already a Python object
        # print(drone_locations)
        # print("type ", type(drone_locations))
     

        # result = []
        # for drone in drone_locations:
           
        #     print(type(drone))
        #     if drone['lat'] == 0 or drone['lon'] == 0:
        #         continue

        #     result.append({
        #         'id': drone.get('droneId', "unknown"),                
        #         'lat': drone['lat'] / 1e7,
        #         'lon': drone['lon'] / 1e7,
        #         'alt': drone['alt'],
        #         'heading': drone['heading']
        #     })

        # return jsonify(result)






      
        # if drone_location["lat"] == 0 or drone_location["lon"] == 0:
        #     return jsonify({'lat': 0, 'lon': 0})
        # return jsonify({
        #     'lat': drone_location["lat"] / 1e7,
        #     'lon': drone_location["lon"] / 1e7,
        #     'alt': drone_location['alt'],
        #     'heading': drone_location['heading'],
        # })


    # GET /flight-hours?drone_id=123456789 → total hours (last 365 days)
    # GET /flight-hours?drone_id=123456789&unit=seconds
    # GET /flight-hours?drone_id=123456789&start=2025-11-01T00:00:00Z&end=2025-11-07T00:00:00Z
    @app.route("/flight-hours")
    def flight_hours():
        drone_id = request.args.get("drone_id")
        unit = request.args.get("unit", "hours").lower()   # "hours" | "seconds"
        start = request.args.get("start")  # optional ISO, e.g. 2025-11-01T00:00:00Z
        end   = request.args.get("end")    # optional ISO

        if not drone_id:
            return jsonify({"error": "drone_id is required"}), 400
        if unit not in ("hours", "seconds"):
            return jsonify({"error": "unit must be 'hours' or 'seconds'"}), 400

        now = datetime.now(timezone.utc)
        end_dt = datetime.fromisoformat(end.replace("Z","+00:00")) if end else now
        start_dt = datetime.fromisoformat(start.replace("Z","+00:00")) if start else (end_dt - timedelta(days=365))

        start_iso = _iso(start_dt)
        end_iso   = _iso(end_dt)

        try:
            total_hours = _flux_sum_hours(drone_id, start_iso, end_iso)
            value = total_hours if unit == "hours" else round(total_hours * 3600.0, 6)
            return jsonify({
                "drone_id": drone_id,
                "start": start_iso,
                "end": end_iso,
                "unit": unit,
                "value": value
            })
        except Exception as e:
            logging.exception("flight-hours endpoint failed")
            return jsonify({"error": f"Failed to query flight hours: {e}"}), 500


    # GET /landings?drone_id=123456789 → last 365 days total
    # GET /landings?drone_id=123456789&start=2025-11-01T00:00:00Z&end=2025-11-07T00:00:00Z
    @app.route("/landings")
    def landings():
        drone_id = request.args.get("drone_id")
        start = request.args.get("start")  # optional ISO, default last 365d
        end   = request.args.get("end")

        if not drone_id:
            return jsonify({"error": "drone_id is required"}), 400

        now = datetime.now(timezone.utc)
        end_dt = datetime.fromisoformat(end.replace("Z","+00:00")) if end else now
        start_dt = datetime.fromisoformat(start.replace("Z","+00:00")) if start else (end_dt - timedelta(days=365))

        start_iso = _iso(start_dt)
        end_iso   = _iso(end_dt)

        try:
            count = _flux_sum_landings(drone_id, start_iso, end_iso)
            return jsonify({
                "drone_id": drone_id,
                "start": start_iso,
                "end": end_iso,
                "landings": count
            })
        except Exception as e:
            logging.exception("landings endpoint failed")
            return jsonify({"error": f"Failed to query landings: {e}"}), 500
        

    @app.route("/crash-alarms")
    def crash_alarms():
        drone_id = request.args.get("drone_id")
        start = request.args.get("start")
        end = request.args.get("end")
        if not drone_id:
            return jsonify({"error":"drone_id is required"}), 400

        now = datetime.now(timezone.utc)
        end_dt = datetime.fromisoformat(end.replace("Z","+00:00")) if end else now
        start_dt = datetime.fromisoformat(start.replace("Z","+00:00")) if start else (end_dt - timedelta(days=30))

        q = f'''
    from(bucket: "{BUCKET}")
    |> range(start: {start_dt.isoformat()}, stop: {end_dt.isoformat()})
    |> filter(fn: (r) => r._measurement == "flight_alarms")
    |> filter(fn: (r) => r.drone_id == "{drone_id}")
    |> filter(fn: (r) => r.event == "crash" or r.event == "hard_landing")
    '''
        df = _qapi.query_data_frame(q)
        if isinstance(df, list) and df: df = pd.concat(df, ignore_index=True)
        if df is None or df.empty:
            return jsonify({"drone_id":drone_id,"items":[]})

        # Return simple list
        items = []
        for _, row in df.iterrows():
            items.append({
                "time": str(row.get("_time")),
                "event": row.get("event"),
                "severity": row.get("severity"),
                "v_down": row.get("v_down"),
                "v_h_pre": row.get("v_h_pre"),
                "g": row.get("g")
            })
        return jsonify({"drone_id":drone_id, "items": items})

    @app.route("/crash-alarms/count")
    def crash_alarms_count():
        drone_id = request.args.get("drone_id")
        start = request.args.get("start")
        end = request.args.get("end")
        if not drone_id: return jsonify({"error":"drone_id is required"}), 400

        now = datetime.now(timezone.utc)
        end_dt = datetime.fromisoformat(end.replace("Z","+00:00")) if end else now
        start_dt = datetime.fromisoformat(start.replace("Z","+00:00")) if start else (end_dt - timedelta(days=365))

        q = f'''
    from(bucket: "{BUCKET}")
    |> range(start: {start_dt.isoformat()}, stop: {end_dt.isoformat()})
    |> filter(fn: (r) => r._measurement == "flight_alarms")
    |> filter(fn: (r) => r.drone_id == "{drone_id}")
    |> group(columns: [])
    |> count(column: "_value")
    '''
        df = _qapi.query_data_frame(q)
        total = 0
        if isinstance(df, list) and df: df = pd.concat(df, ignore_index=True)
        if df is not None and not df.empty:
            # count of alarm rows
            try: total = int(df["_value"].sum())
            except: total = 0
        return jsonify({"drone_id":drone_id, "count": total})     

 
    # GET /online-devices → returns JSON { "count": N, "devices":[{"drone_id":"123", "last_seen":"..."}...] }
    # GET /online-devices?ttl=600 → consider devices offline if last seen older than 600 seconds.
    @app.route("/online-devices")
    def online_devices():
        """
        Query and return currently online drone ids.
        optional query param: ttl (seconds) to consider a device stale (default 900s)
        """
        ttl = request.args.get("ttl", default=900, type=int)
        try:
            devices = _flux_online_devices(ttl_seconds=ttl)
            return jsonify({"count": len(devices), "devices": devices})
        except Exception as e:
            logging.exception("online-devices endpoint failed")
            return jsonify({"error": str(e)}), 500
  



    @app.route("/online-devices-fast")
    def online_devices_fast():
        use_cache = request.args.get("cache", "1") == "1"
        if use_cache and hasattr(publisher, "_online_devices"):
            devices = [{"drone_id": d, "last_seen": None} for d in sorted(publisher._online_devices)]
            return jsonify({"count": len(devices), "devices": devices})
        return online_devices()
    


    @app.route("/nbirth")
    def get_nbirth_by_drone():
        """
        GET /nbirth?drone_id=123456789
        Prefer in-memory publisher.client.online_devices; fall back to InfluxDB 'device_birth'.
        """
        drone_id = request.args.get("drone_id")
        if not drone_id:
            return jsonify({"error": "missing drone_id query parameter"}), 400

        # 1) Try in-memory online_devices (fast path)
        try:
            # publisher should be available in the closure of register_routes
            mqtt_client = getattr(publisher, "client", None) or getattr(publisher, "mqtt_client", None)
            online = None
            if mqtt_client is not None:
                online_devices = getattr(mqtt_client, "online_devices", None)
                if isinstance(online_devices, dict):
                    rec = online_devices.get(drone_id)
                    if rec:
                        # return friendly response built from in-memory record
                        payload = rec.get("payload") or {}
                        last_seen = rec.get("last_seen") or payload.get("start_time") or datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
                        resp = {
                            "found": True,
                            "source": "memory",
                            "drone_id": drone_id,
                            "last_seen": last_seen,
                            "hostname": payload.get("system", {}).get("hostname"),
                            "ip_address": payload.get("system", {}).get("ip_address"),
                            "uptime_seconds": try_parse_number(payload.get("system", {}).get("uptime_seconds")),
                            "cpu_percent": try_parse_number(payload.get("system", {}).get("cpu_percent")),
                            "mem_total_mb": try_parse_number((payload.get("system", {}) or {}).get("memory", {}).get("total_mb")),
                            "mem_used_mb": try_parse_number((payload.get("system", {}) or {}).get("memory", {}).get("used_mb")),
                            "mem_percent": try_parse_number((payload.get("system", {}) or {}).get("memory", {}).get("percent")),
                            "deployments": payload.get("deployments")  # already structured if stored from NBIRTH
                        }
                        return jsonify(resp), 200
        except Exception as e:
            logging.warning(f"Error checking in-memory online_devices: {e}")
            # fall back to DB

        # 2) Fallback to InfluxDB (persistent)
        try:
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            qapi = client.query_api()

            # Query latest device_birth records for this drone_id
            # Using last() will return the last point per field; range uses a long window to be safe
            flux_q = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -3650d)
    |> filter(fn: (r) => r._measurement == "device_birth")
    |> filter(fn: (r) => r.drone_id == "{drone_id}")
    |> last()
    '''
            df = qapi.query_data_frame(flux_q)

            if isinstance(df, list):
                df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()

            if df is None or df.empty:
                client.close()
                return jsonify({"found": False, "drone_id": drone_id}), 200

            # pivot so each _field becomes a column (one row)
            try:
                pivot = df.pivot_table(index=['drone_id', '_time'], columns='_field', values='_value', aggfunc='first').reset_index()
                pivot['_time'] = pd.to_datetime(pivot['_time'], utc=True)
                latest = pivot.loc[pivot['_time'].idxmax()]
                last_seen = latest['_time'].to_pydatetime().astimezone(timezone.utc).isoformat().replace("+00:00","Z")

                out = {
                    "found": True,
                    "source": "influx",
                    "drone_id": str(latest.get('drone_id')),
                    "last_seen": last_seen
                }

                # map of expected fields -> output key
                field_map = {
                    'hostname': 'hostname',
                    'ip_address': 'ip_address',
                    'uptime_seconds': 'uptime_seconds',
                    'cpu_percent': 'cpu_percent',
                    'mem_total_mb': 'mem_total_mb',
                    'mem_used_mb': 'mem_used_mb',
                    'mem_percent': 'mem_percent',
                    'deployments_json': 'deployments'
                }

                for col, outkey in field_map.items():
                    if col in latest.index and pd.notna(latest.get(col)):
                        val = latest.get(col)
                        if col == 'deployments_json':
                            out[outkey] = try_parse_json(val)
                        else:
                            out[outkey] = try_parse_number(val) if isinstance(val, (int, float, str)) else val

                client.close()
                return jsonify(out), 200

            except Exception as ex_pivot:
                # fallback to manual row scan if pivot fails
                logging.warning(f"Pivot failed for NBIRTH query: {ex_pivot}; trying manual aggregation")
                df['_time'] = pd.to_datetime(df['_time'], utc=True)
                df = df.sort_values('_time', ascending=False)
                # find latest timestamp row for each field (we'll assemble a dict)
                result = {"found": True, "source": "influx", "drone_id": drone_id}
                # take the newest timestamp overall as last_seen
                last_ts = df.iloc[0]['_time']
                result['last_seen'] = pd.to_datetime(last_ts, utc=True).astimezone(timezone.utc).isoformat().replace("+00:00","Z")

                # collect values by field taking the first (which is the latest due to sort)
                field_vals = {}
                for _, row in df.iterrows():
                    fld = row.get('_field')
                    if fld and fld not in field_vals:
                        field_vals[fld] = row.get('_value')

                # map common fields back into result
                if 'hostname' in field_vals: result['hostname'] = field_vals['hostname']
                if 'ip_address' in field_vals: result['ip_address'] = field_vals['ip_address']
                if 'uptime_seconds' in field_vals: result['uptime_seconds'] = try_parse_number(field_vals['uptime_seconds'])
                if 'cpu_percent' in field_vals: result['cpu_percent'] = try_parse_number(field_vals['cpu_percent'])
                if 'mem_total_mb' in field_vals: result['mem_total_mb'] = try_parse_number(field_vals['mem_total_mb'])
                if 'mem_used_mb' in field_vals: result['mem_used_mb'] = try_parse_number(field_vals['mem_used_mb'])
                if 'mem_percent' in field_vals: result['mem_percent'] = try_parse_number(field_vals['mem_percent'])
                if 'deployments_json' in field_vals: result['deployments'] = try_parse_json(field_vals['deployments_json'])

                client.close()
                return jsonify(result), 200

        except Exception as e:
            logging.exception(f"Failed to query NBIRTH for {drone_id}: {e}")
            try:
                client.close()
            except:
                pass
            return jsonify({"error": "internal error"}), 500    



    @app.route("/stats.html")
    def stats_page():
        return send_from_directory("static", "stats.html")
