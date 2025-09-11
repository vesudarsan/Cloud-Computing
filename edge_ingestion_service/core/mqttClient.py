import paho.mqtt.client as mqtt
import json
import time
import psutil
import socket
import platform
from utils.logger import setup_logger
from utils.influx_writer import InfluxWriter

from utils.rest_client import RestClient
logging = setup_logger(__name__)


# InfluxDB Config
INFLUX_TOKEN = "drone-secret-token"
INFLUX_ORG = "drone-org"
INFLUX_BUCKET = "drone_telemetry"

if platform.system() == "Windows": # 2dl read from config
    INFLUX_URL = "http://localhost:8086"
else:
    INFLUX_URL = "http://host.docker.internal:8086"

class MQTTClient:
    def __init__(self, broker, port, drone_id,sparkplug_namespace,
                            sp_group_id,sp_edge_id,sp_device_id):       
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
        # self.TOPIC_PREFIX = f"{sparkplug_namespace}/{sp_group_id}/+/{sp_edge_id}"
        self.TOPIC_PREFIX = f"{sparkplug_namespace}/{sp_group_id}/NCMD/{sp_edge_id}"
        self.rest_client = RestClient()
        self.influx_writer = InfluxWriter(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            bucket=INFLUX_BUCKET)


 

    def connect(self,topic,lwt_message,qos=1,retain=True):
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.will_set(topic,lwt_message,qos,retain)        
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()

    def disconnect(self):
        disconnect_msg = json.dumps({
            "drone_id": self.drone_id,
            "status": "disconnect",
            "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        })

        topic = f"{self.sparkplug_namespace}/{self.sp_group_id}/NDEATH/{self.sp_edge_id}"
        self.client.publish(topic, payload=disconnect_msg, qos=1, retain=True)

        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.influx_writer.close() 
        finally:
            self.connected = False
            logging.info("✅ MQTT disconnected")



    # def publish_birth_message(self):    

    #     resp = self.rest_client.get(ota_url+"/containers")

             
    #     birth_msg = json.dumps({
    #         "drone_id": self.drone_id,
    #         "status": "online",
    #         "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    #          "system": self.get_system_info(),
    #          "deployments": (self.rest_client.get(ota_url+"/containers")).json() # get this from OTA serive           
    #     })

    #     topic = f"{self.sparkplug_namespace}/{self.sp_group_id}/NBIRTH/{self.sp_edge_id}"
    #     self.client.publish(topic, payload=birth_msg, qos=1, retain=True)

    #     logging.info("Published MQTT birth message")        

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)

        if self.connected:            
            logging.info(f"✅ Connected to MQTT broker [{self.broker}:{self.port}] with code {rc}")
        else:           
            logging.error(f"❌ Failed to connect to MQTT broker [{self.broker}:{self.port}] with code {rc}")
            return

        # Publish NBIRTH (only if Sparkplug fields exist) # 2dl check later
        # try:
        #     self.publish_birth_message()
        # except Exception as e:
        #     logging.error(f"⚠️ Failed to publish NBIRTH: {e}") 

        # Define topics to subscribe
        topics = [
   
            f"{self.sparkplug_namespace}/{self.sp_group_id}/NBIRTH/{self.sp_edge_id}/#",
            f"{self.sparkplug_namespace}/{self.sp_group_id}/DDATA/{self.sp_edge_id}/Mavlink"
        ]

        # Subscribe and log each topic
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

    def update_drone_location(self,droneId,payload):

        self.drone_location['droneId'] = droneId
        self.drone_location['lat'] = payload['lat']
        self.drone_location['lon'] = payload['lon']
        self.drone_location['alt'] = payload['alt']
        self.drone_location['heading'] = payload['hdg']

    def get_drone_location(self):
        return self.drone_location


    def _on_message(self, client, userdata, msg):
               
        try:
            #payload_str = msg.payload.decode("utf-8")
            logging.debug(f"📩 Received message on {msg.topic}: {msg.payload.decode('utf-8')}")
       
            topic = msg.topic
            droneId = topic.split("/")[-2]
       
            message_type = None
            if topic.endswith("/Mavlink"):
                payload = json.loads(msg.payload.decode())                
                message_type = payload.get("messageType")                
                self.influx_writer.write_points(payload,droneId,message_type)
            
            if message_type == "GLOBAL_POSITION_INT":
                self.update_drone_location(droneId,payload)
              

           
            # if topic.endswith("/deploy"):
            #     image = payload.get("image")
            #     name = payload.get("name")
            #     ports = payload.get("ports")
            #     if image and name:
            #         self.rest_client.post(ota_url+"/deploy", payload)                   
            #     else:
            #         logging.error(f"Missing 'image' or 'name' in deploy payload")
                  

            # elif topic.endswith("/start"):
            #     name = payload.get("name")
            #     if name is not None:
            #         # Send as JSON payload                   
            #         resp = self.rest_client.post(f"{ota_url}start", payload)                    
            #         logging.info(f"Start response:{resp.status_code}")
                   
            #     else:
            #         logging.info(f"⚠️ Start command received but no 'name' in payload:{payload}")

            # elif topic.endswith("/stop"):              
            #     name = payload.get("name")  # will be None if key not present              
               
            #     if name is not None:
            #         # Send as JSON payload                   
            #         resp = self.rest_client.post(f"{ota_url}stop", payload)                    
            #         logging.info(f"Stop response:{resp.status_code}")
                   
            #     else:
            #         logging.info(f"⚠️ Stop command received but no 'name' in payload:{payload}")
                  

            # elif topic.endswith("/restart"):
            #     name = payload.get("name")
            #     if name is not None:
            #         # Send as JSON payload                   
            #         resp = self.rest_client.post(f"{ota_url}restart", payload)                    
            #         logging.info(f"restart response:{resp.status_code}")
                   
            #     else:
            #         logging.info(f"⚠️ restart command received but no 'name' in payload:{payload}")




            # Only process messages for MAVLINK topics      
            # if "MAVLINK" not in msg.topic.upper():
            #     logging.info("⏭ Skipping message (not a MAVLINK topic)")
            #     return

            # data = json.loads(payload_str)


        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in payload: {e}")
        # except Exception as e:
        #     logging.error(f"Error processing message: {e}")




    def publish(self, topic=None, payload =None, qos=1,storeAndForward = False):   
        actual_topic = topic or self.topic
        if not self.connected:
            logging.warning("❌ MQTT not connected")
            return None
        
        logging.info(f"✅ Published to {actual_topic} [qos={qos}]")
        return self.client.publish(actual_topic, payload, qos=qos)



    def is_connected(self):
        return self.connected