from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}

def get_latest_run():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        SELECT
            run_timestamp,
            ts_temperature,
            ts_humidity,
            mqtt_temperature,
            mqtt_humidity
        FROM iot_run_data
        ORDER BY run_timestamp DESC
        LIMIT 1;
    """

    cursor.execute(query)
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row is None:
        return None

    return {
        "run_timestamp": row[0].isoformat(),
        "thingspeak": {
            "temperature": row[1],
            "humidity": row[2],
        },
        "mqtt": {
            "temperature": row[3],
            "humidity": row[4],
        },
    }

@app.route("/latest", methods=["GET"])
def latest_data():
    data = get_latest_run()
    if data is None:
        return jsonify({"error": "No data found"}), 404
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
