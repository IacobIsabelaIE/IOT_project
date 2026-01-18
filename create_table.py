import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="airflow",
    user="airflow",
    password="airflow"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS iot_run_data (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    ts_temperature FLOAT,
    ts_humidity FLOAT,
    mqtt_temperature FLOAT,
    mqtt_humidity FLOAT,
    llm_summary TEXT
);
""")

conn.commit()
cursor.close()
conn.close()

print("Table iot_readings is ready.")
