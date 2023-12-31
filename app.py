import json
import random
import sys
import paramiko
from flask_api.exceptions import ParseError
from pymysql import ProgrammingError, Connection
from typing_extensions import Literal
import pymysql.cursors
from flask import Flask, request, jsonify, Request
from flask_api import status
import sshtunnel
from pythonping import ping


master_node = '54.197.137.34'
data_node_1 = '50.17.119.193'
data_node_2 = '3.85.126.61'
data_node_3 = '54.147.55.185'



def get_data(user_request: Request):
    """
    read the data received in the user's request and check if it follows a certain form, otherwise raise a parse error.

    :param user_request: The user request
    :return: The data as a dictionary
    """
    
    if user_request.is_json:
        data = user_request.get_json()
        if 'SQL' in data and 'Mode' in data and data['Mode'] in ['Direct', 'Random', 'Customized']:
            return data
    raise ParseError


def forward_request(mode: Literal['Direct', 'Random', 'Customized'], query: str, query_type: Literal['Read', 'Write']):
    """
    forward the user's requests according to the chosen mode. If "Direct" mode, all requests are sent to the master node.
    If "Random" mode, write requests are sent to the master node while read requests are sent to the randomly chosen data node.
    If "Customized" mode, write requests are sent to the master node while read requests are sent to the data node which has
    the lowest response time.

    :param mode: The request transfer mode ('Direct', 'Random', 'Customized')
    :param query: The SQL query
    :param query_type: The query type (Read or Write)
    :return: The query result
    """
    if mode == 'Direct':
        with pymysql.connect(
                host=master_node,
                user='root',
                password='root',
                database='sakila',
                cursorclass=pymysql.cursors.DictCursor
        ) as connection:
            return run_query(connection, query, query_type)
    else:
        if mode == 'Random':
            data_node = random.choice([data_node_1, data_node_2,data_node_3])
            print(data_node)
        else:  # Customized
            data_node = get_fastest_data_node([data_node_1, data_node_2,data_node_3])
            
        paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)


        with sshtunnel.open_tunnel(
                (data_node, 22),
                ssh_username="ubuntu",
                ssh_pkey="/home/ubuntu/MyKeyPair.pem",
                remote_bind_address=(master_node, 3306),
                local_bind_address=('0.0.0.0', 3306)
        ) as tunnel:
            with pymysql.connect(
                    host=tunnel.local_bind_host,
                    port=tunnel.local_bind_port,
                    user='root',
                    password='root',
                    database='sakila',
                    cursorclass=pymysql.cursors.DictCursor
            ) as connection:
                return run_query(connection, query, query_type)


def run_query(connection: Connection, query: str, query_type: Literal['Read', 'Write']):
    """
    run the SQL query on the node specified by the connection link

    :param connection: The connection link to the node on which we want to run the SQL query.
    :param query: The SQL query
    :param query_type: The query type (Read or Write)
    :return: The query result
    """
    with connection.cursor() as cursor:
        try:
            affected_rows = cursor.execute(query)
            if query_type == 'Read':
                result = cursor.fetchall()
            else:
                connection.commit()
                result = f'Affected rows: {affected_rows}'
        except ProgrammingError as e:
            return jsonify(e.args), status.HTTP_400_BAD_REQUEST
        except Exception as e:
            return jsonify(e.args), status.HTTP_500_INTERNAL_SERVER_ERROR
        else:
            return jsonify(result), status.HTTP_200_OK


def get_fastest_data_node(data_nodes: list):
    """
    determine which node has the fastest response time by performing pings

    :param data_nodes: The list of nodes (private IPv4 addresses)
    :return: The node with the fastest response time
    """
    response_time = float('inf')
    index = None

    for i, data_node in enumerate(data_nodes):
        response = ping(data_node)
        if response.rtt_avg_ms < response_time:
            response_time = response.rtt_avg_ms
            index = i

    return data_nodes[index]


# Launch Flask app
app = Flask(__name__)

# Load private IPv4 addresses of nodes


master_node = '54.197.137.34'
data_node_1 = '50.17.119.193'
data_node_2 = '3.85.126.61'
data_node_3 = '54.147.55.185'


@app.route('/', methods=['GET'])
def proxy_is_working():
    """
    Returns a simple response to indicate that the proxy server is working correctly

    :return: A message indicating that the proxy server is working correctly
    """
    return "The proxy server is working properly :)", status.HTTP_200_OK


@app.route('/read', methods=['GET'])
def read_request():
    try:
        print("m hereeeeeeeeeeeeeeee")
        data = get_data(request)
        mode = data['Mode']
        print("mode is "+mode)
        query = data['SQL']
        print("query is "+query)
        return forward_request(mode, query, 'Read')
    except ParseError as e:
        return e.detail, e.status_code
    except Exception as e:
        # Log the exception for debugging
        print(f"Error: {e}")
        # Return a more informative error message
        return jsonify({'error': str(e)}), status.HTTP_500_INTERNAL_SERVER_ERROR


@app.route('/write', methods=['POST', 'PUT', 'DELETE'])
def write_request():
    """
    As all write requests must be processed by the master node, so "Direct" mode is applied to all these requests.

    :return: The query result
    """
    try:
        data = get_data(request)
        query = data['SQL']
    except ParseError as e:
        return e.detail, e.status_code
    except Exception as e:
        return jsonify(e.args), status.HTTP_500_INTERNAL_SERVER_ERROR
    else:
        return forward_request('Direct', query, 'Write')

if __name__ == "__main__":
    # Run the Flask app on localhost and port 5000
    app.run(host="0.0.0.0", port=5000)