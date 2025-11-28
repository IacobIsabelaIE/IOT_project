from flask_cors import CORS
from flask import Flask, jsonify, render_template
import requests
import paho.mqtt.client as mqtt
import json
from threading import Lock

app = Flask(__name__)
CORS(app)

# Store the latest MQTT data
latest_data = {}
data_lock = Lock()

# MQTT Configuration
MQTT_BROKER = "192.168.1.132"  # Replace with your broker address
MQTT_PORT = 1883
MQTT_TOPIC = "arduino/dht11"

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    global latest_data
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        
        with data_lock:
            latest_data = data
        
        print(f"Received message: {data}")
    except Exception as e:
        print(f"Error processing message: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
except Exception as e:
    print(f"Could not connect to MQTT broker: {e}")

# -------------------------
# SERVE FRONT-END FROM FLASK
# -------------------------
@app.route('/')
def home():
    return render_template('index.html')

# Flask Routes
@app.route('/fetch-data')
def fetch_data():
    api_url = 'https://thingspeak.mathworks.com/channels/357142/feeds.json?'
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        return jsonify(data)
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500

@app.route('/mqtt-data')
def get_mqtt_data():
    with data_lock:
        if latest_data:
            return jsonify(latest_data)
        else:
            return jsonify({'error': 'No data received yet'}), 404

if __name__ == '__main__':
    try:
        app.run(debug=True, use_reloader=False)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()