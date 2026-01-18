from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import requests
import json
from openai import OpenAI
import paho.mqtt.client as mqtt
from threading import Event

MQTT_BROKER = "host.docker.internal"
MQTT_PORT = 1883
MQTT_TOPIC = "arduino/dht11"

def fetch_thingspeak(**context):
    url = "https://thingspeak.mathworks.com/channels/357142/feeds.json?"
    response = requests.get(url)
    context['ti'].xcom_push(key="get_thingspeak", value=response.json())

def extract_last_thingspeak_entry(**context):
    ti = context["ti"]
    data = ti.xcom_pull(key="get_thingspeak", task_ids="fetch_thingspeak")

    latest_feed = data["feeds"][-1]
    temperature = float(latest_feed["field3"])
    humidity = float(latest_feed["field5"])

    latest = {
        "temperature": temperature,
        "humidity": humidity
    }

    print("Latest thingspeak:", latest)

    context['ti'].xcom_push(key="latest_thingspeak", value=latest)


def fetch_mqtt(**context):
    data_event = Event()
    mqtt_data = {}

    def on_message(client, userdata, msg):
        nonlocal mqtt_data
        mqtt_data = json.loads(msg.payload.decode())
        data_event.set()

    client = mqtt.Client()
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    client.loop_start()

    data_event.wait(timeout=10)
    client.loop_stop()
    client.disconnect()

    if not mqtt_data:
        raise ValueError("No MQTT data received")

    context['ti'].xcom_push(key='mqtt', value=mqtt_data)

def generate_llm_summary(**context):
    ti = context["ti"]

    thingspeak = ti.xcom_pull(
        key="latest_thingspeak",
        task_ids="extract_last_thingspeak_entry"
    )

    mqtt = ti.xcom_pull(
        key="mqtt",
        task_ids="fetch_mqtt"
    )

    if not thingspeak or not mqtt:
        raise ValueError("Missing data for LLM")

    prompt = f"""
    You are an IoT monitoring assistant.

    ThingSpeak data (from Stuttgard, Germany):
    - Temperature: {thingspeak['temperature']} °C
    - Humidity: {thingspeak['humidity']} %

    MQTT sensor data (from Cluj-Napoca, Romania):
    - Temperature: {mqtt['temperature']} °C
    - Humidity: {mqtt['humidity']} %

    Write a short natural-language summary of the current environment.
    """

    client = OpenAI()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You summarize IoT sensor data."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3
    )

    summary = response.choices[0].message.content

    print("LLM summary:", summary)

    ti.xcom_push(key="llm_summary", value=summary)


def combine_and_store_data(**context):
    ti = context['ti']

    thingspeak_data = ti.xcom_pull(
        key='latest_thingspeak',
        task_ids='extract_last_thingspeak_entry'
    )
    mqtt_data = ti.xcom_pull(
        key='mqtt',
        task_ids='fetch_mqtt'
    )

    llm_summary = ti.xcom_pull(
        key="llm_summary",
        task_ids="generate_llm_summary"
    )


    if not thingspeak_data or not mqtt_data or not llm_summary:
        raise ValueError("Missing data for DB insert")

    sql = """
        INSERT INTO iot_run_data (
            run_timestamp,
            ts_temperature,
            ts_humidity,
            mqtt_temperature,
            mqtt_humidity,
            llm_summary
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    params = (
        context["logical_date"],
        thingspeak_data["temperature"],
        thingspeak_data["humidity"],
        mqtt_data["temperature"],
        mqtt_data["humidity"],
        llm_summary
    )

    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(sql, parameters=params)

# ---------------------------
# DAG definition
# ---------------------------
with DAG(
    dag_id='iot_data_orchestration',
) as dag:

    t1 = PythonOperator(
        task_id='fetch_thingspeak',
        python_callable=fetch_thingspeak
    )

    t2 = PythonOperator(
        task_id='fetch_mqtt',
        python_callable=fetch_mqtt
    )

    t3 = PythonOperator(
        task_id="extract_last_thingspeak_entry",
        python_callable=extract_last_thingspeak_entry
    )

    t4 = PythonOperator(
        task_id="generate_llm_summary",
        python_callable=generate_llm_summary
    )

    t5 = PythonOperator(
        task_id='combine_and_store_data',
        python_callable=combine_and_store_data
    )

    [t1, t2] >> t3 >> t4 >> t5 # type: ignore
