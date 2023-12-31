#gatekeeper codee
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Replace with the actual URL of the next host
NEXT_HOST_URL = 'http://ip_address:port'


def verify_mode(query_data):
    """
    Verify that the mode is either 'Random' or 'Direct'.
    Raise ValueError if the mode is invalid.
    """
    mode = query_data.get("Mode", "").lower()
    if mode not in ["random", "direct"]:
        raise ValueError("Invalid Mode")

@app.route('/read', methods=['GET'])
def handle_read_query():
    query_data = request.json
    try:
        verify_mode(query_data)
        # Forward the query to the next host without modification
        response = requests.get(f"{NEXT_HOST_URL}/read", json=query_data)
        return jsonify(response.json()), response.status_code
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@app.route('/write', methods=['POST'])
def handle_write_query():
    query_data = request.json
    try:
        verify_mode(query_data)
        # Forward the query to the next host without modification
        response = requests.post(f"{NEXT_HOST_URL}/write", json=query_data)
        return jsonify(response.json()), response.status_code
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
