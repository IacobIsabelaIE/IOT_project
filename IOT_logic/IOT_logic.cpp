#include <Arduino.h>
#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_Sensor.h>
#include <DHT.h>


// ---- WiFi Settings ----
const char* ssid = "DIGI-H6fH";
const char* password = "U5GP6uqEM7";

// ---- MQTT Broker ----
const char* mqtt_server = "192.168.1.132";

// ---- DHT Sensor ----
#define DHTPIN 5
#define DHTTYPE DHT11
DHT dht(DHTPIN, DHTTYPE);

// MQTT client objects
WiFiClient espClient;
PubSubClient client(espClient);

// Timer
unsigned long lastPublish = 0;
const long interval = 2000; 

// ---- WiFi Connection ----
void setup_wifi() {
  delay(10);
  Serial.println();
  Serial.println("Connecting to WiFi...");
  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println("");
  Serial.println("WiFi connected!");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

// ---- Reconnect to MQTT if needed ----
void reconnect() {
  while (!client.connected()) {
    Serial.print("Attempting MQTT connection...");
    if (client.connect("DHT11_ESP32_Client")) {
      Serial.println("connected to MQTT!");
    } else {
      Serial.print("failed, rc=");
      Serial.println(client.state());
      Serial.println("Trying again in 2 seconds...");
      delay(2000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  dht.begin();

  setup_wifi();

  client.setServer(mqtt_server, 1883);

  Serial.println("System started...");
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }

  client.loop();

  unsigned long now = millis();
  if (now - lastPublish >= interval) {
    lastPublish = now;

    float temp = dht.readTemperature();
    float hum = dht.readHumidity();

    if (isnan(temp) || isnan(hum)) {
      Serial.println("Failed to read from DHT sensor!");
      return;
    }

    // ---- Print locally ----
    Serial.print("Temperature: ");
    Serial.print(temp);
    Serial.print(" °C | Humidity: ");
    Serial.print(hum);
    Serial.println(" %");

    // ---- Publish to MQTT ----
    char payload[50];
    sprintf(payload, "{\"temperature\": %.2f, \"humidity\": %.2f}", temp, hum);
    client.publish("arduino/dht11", payload);

    Serial.print("MQTT Published: ");
    Serial.println(payload);
  }
}
