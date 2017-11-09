#-*- coding: utf-8 -*-
"""
  © Copyright 2016, Dell, Inc.  All Rights Reserved.
  Author: Shreya Gupta
  Last Modified: 11/07/2017
  Python REST server that subscribes to EdgeX services, collects data and sends
  it to a WebSocket Server to pipe to Unity.
"""

import json
import logging
import time
from threading import Thread

from flask import Flask, request
from flask_restful import Resource, Api
import requests
import websocket


app = Flask(__name__)
api = Api(app)
cache = dict()
cache['type'] = 'reading'
WS_SERVER_URI = 'ws://10.0.4.220:9001'
DATA_URI = 'http://10.0.4.62:48080/api/v1/'
COMMAND_URI = 'http://10.0.4.62:48082/api/v1/'


# command = {'type': 'command',
#            'RPM': 0,
#            'name': 'motor'}
command = {'type': 'command',
           'RedLightControlState': 1,
           'name': 'patlite'}


def config_flask():
    api.add_resource(Data, "/data")


def get_uom():
    r = requests.get(DATA_URI + 'valuedescriptor')
    parsed = r.json()
    uom = dict()
    for value in parsed:
        name = value['name']
        uom[name] = value['uomLabel']
    print(json.dumps(uom, indent=4, sort_keys=True))
    return uom


def scrub_meta(metadata):
    """Scrubs unneeded device metadata."""
    scrubbed = dict()
    for device in metadata:
        device_name = device['name']
        scrubbed[device_name] = dict()
        for command in device['commands']:
            command_name = command['name']
            if command['put'] is not None:
                scrubbed[device_name][command_name] = dict()
                url = command['put']['url']
                parameterNames = command['put']['parameterNames']
                scrubbed[device_name][command_name]['url'] = url
                scrubbed[device_name][command_name]['parameterNames'] = \
                    parameterNames
    return scrubbed


def get_device_meta():
    r = requests.get(COMMAND_URI + 'device')
    return scrub_meta(r.json())


def find_command(command):
    """Finds the command to send to EdgeX."""
    for each in command:
        if each != 'name':
            return each


def update_device(command):
    """Sends request to EdgeX device service to update value."""
    device_name = command['name']
    if device_name in device_meta:
        command_name = find_command(command)
        for each_command in device_meta[device_name]:
            print(each_command)
            if command_name in device_meta[device_name][each_command]['parameterNames']:
                url = device_meta[device_name][each_command]['url']
                data = '{' + command_name + ':' \
                    + str(command[command_name]) + '}'
                print(url, data)
                r = requests.put(url, data=data)
                print("Status code for command:", r.status_code)
                r = requests.get(url)


def run_thread(func_to_run, args=None):
    if args == None:
        thread = Thread(target=func_to_run)
    else:
        thread = Thread(target=func_to_run, args=args)
    thread.daemon = True
    thread.start()


def on_message(ws, message):
    try:
        command = json.loads(message)
    except ValueError:
        print('Message received from server not in correct JSON format')
        return
    if 'type' in command and command['type'] == 'command':
        print('Command received: ', command)
        del command['type']
        run_thread(update_device, (command,))


def on_error(ws, error):
    print('Error is: ', error)


def on_close(ws):
    print('### Connection closed ###')


def on_open(ws):
    def run():
        while True:
            time.sleep(1)
    run_thread(run)


def start_ws_client(ws):
    ws.on_open = on_open
    while True:
        try:
            ws.run_forever()
        except:
            time.sleep(1)


def send_data():
    """Sends cache to WebSocket Server."""
    global cache
    message = json.dumps(cache)
    ws.send(message)
    print('Sent updated cache')


def is_valid(data):
    """Checks if the structure of data follows the format below or not.

    {
        'device': <string>,
        'readings': [
            {
                'name': <string>,
                'value': <string>,...
            },...
        ]
    }
    """
    if 'device' not in data or 'readings' not in data:
        return False
    elif data['readings'] == []:
        return False
    else:
        for reading in data['readings']:
            if 'name' not in reading or 'value' not in reading:
                return False
    return True


class Data(Resource):
    """Parses data received from gateway."""
    def _prettify(self, reading_name, value):
        """Makes value of device parameter pretty."""
        curr_uom = uom[reading_name]
        return value[:6] + ' ' + curr_uom

    def _has_updated(self, new_data):
        """Checks if device readings have updated."""
        device_name = new_data['device']
        for reading in new_data['readings']:
            reading_name = reading['name']
            if reading_name not in cache[device_name] or \
                reading['value'] != cache[device_name][reading_name]:
                return True
        return False

    def _update_cache(self, data):
        """Updates cache if needed, returns True if it did update else False."""
        device_name = data['device']
        if device_name not in cache:
            self._create_device(device_name)
        if self._has_updated(data):
            for reading in data['readings']:
                device_name = data['device']
                for reading in data['readings']:
                    reading_name = reading['name']
                    cache[device_name][reading_name] = self._prettify(
                        reading_name,
                        reading['value']
                    )
            return True
        return False

    def _create_device(self, device_name):
        """Creates new device in cache."""
        cache[device_name] = dict()

    def post(self):
        """Data subscription endpoint for EdgeX Command Distro Service."""
        try:
            data = json.loads(request.data)
        except ValueError:
            return "Data not in valid JSON format!", 400
        if not is_valid(data):
            return "Data does not follow schema", 400
        if self._update_cache(data):
            run_thread(send_data)


@app.route('/')
def hello():
    """Root URI endpoint."""
    return 'The app is running!'


config_flask()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    uom = get_uom()
    device_meta = get_device_meta()
    ws = websocket.WebSocketApp(WS_SERVER_URI,
                                on_message=on_message,
                                on_close=on_close,
                                on_error=on_error)
    run_thread(start_ws_client, (ws,))
    port = 5000
    app.run(debug=False, port=int(port), host='0.0.0.0')
