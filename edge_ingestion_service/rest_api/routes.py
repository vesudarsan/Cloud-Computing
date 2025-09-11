from flask import Flask, request, jsonify,render_template,send_from_directory
from utils.logger import setup_logger
from flask_cors import CORS
import os

logging = setup_logger(__name__)

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
        drone_location = publisher.client.get_drone_location() 
        print(drone_location)
      
        if drone_location["lat"] == 0 or drone_location["lon"] == 0:
            return jsonify({'lat': 0, 'lon': 0})
        return jsonify({
            'lat': drone_location["lat"] / 1e7,
            'lon': drone_location["lon"] / 1e7,
            'alt': drone_location['alt'],
            'heading': drone_location['heading'],
        })