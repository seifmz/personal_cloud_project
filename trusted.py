#trusted host code 
from flask import Flask, request, jsonify
import requests
import re

app = Flask(__name__)

# Replace with the actual URL of the next host
NEXT_HOST_URL = 'http://ip_address:port'


def verify_mode(query_data):
    """
    Verify that the mode is either 'Random' or 'Direct'.
    """
    mode = query_data.get("Mode", "").lower()
    return mode in ["random", "direct"]

def verify_sql_query(sql_query):
    """
    Perform basic verifications on the SQL query.
    """
    pattern = r"^(SELECT \* FROM [a-zA-Z0-9_\.]+|INSERT INTO [a-zA-Z0-9_\.]+ \([a-zA-Z0-9_, ]+\) VALUES \(.+\))$"
    return bool(re.match(pattern, sql_query, re.IGNORECASE))

@app.route('/read', methods=['GET'])
def handle_read_query():
    query_data = request.json
    if not (verify_mode(query_data) and verify_sql_query(query_data.get("SQL", ""))):
        return jsonify({"error": "Invalid request"}), 400

    response = requests.get(f"{NEXT_HOST_URL}/read", json=query_data)
    return jsonify(response.json()), response.status_code

@app.route('/write', methods=['POST'])
def handle_write_query():
    query_data = request.json
    if not (verify_mode(query_data) and verify_sql_query(query_data.get("SQL", ""))):
        return jsonify({"error": "Invalid request"}), 400

    response = requests.post(f"{NEXT_HOST_URL}/write", json=query_data)
    return jsonify(response.json()), response.status_code

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
