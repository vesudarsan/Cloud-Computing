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

    @app.route("/control.html") # 2dl update later
    def serve_page():
        return send_from_directory("static", "control.html")

  
     
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

    @app.route("/stats.html")
    def stats_page():
        return send_from_directory("static", "stats.html")
