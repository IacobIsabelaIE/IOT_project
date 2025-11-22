from flask_cors import CORS
from flask import Flask, jsonify
import requests

app = Flask(__name__)
CORS(app)
@app.route('/fetch-data')
def fetch_data():

    api_url = 'https://api.thingspeak.com/channels/9/feeds.json?'

    try:
        response = requests.get(api_url)
        response.raise_for_status()  
        data = response.json() 
        return jsonify(data)
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
