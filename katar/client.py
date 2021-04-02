import socket
import json
import time

class Publish(object):
    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.socket = None
        self.subscriptions = []

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(30)
        self.socket.connect((self.server, self.port))

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_message(self, instruction, payload):
        message = instruction + " "
        if isinstance(payload, dict):
            message += json.dumps(payload)

        if not isinstance(message, bytes):
            message = message.encode('utf-8')

        print("Sending -",message)

        try:
            self.socket.send(message)
        except socket.error:
            # attempt to reconnect if there was a connection error
            self.close()
            self.connect()
            self.socket.send(message)

class Subscribe(object):
    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.socket = None
        self.subscriptions = []

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(30)
        self.socket.connect((self.server, self.port))

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def get_message(self):
        self.socket.settimeout(1)
        data = self.socket.recv(1024).decode('utf-8')
        queue, message = data.split(' ', 1)
        print("Raw Data -",data,"\n")
        return queue, json.loads(message)

    def subscribe(self, payload):
        message = "subscribe "
        if isinstance(payload, dict):
            message += json.dumps(payload)

        if not isinstance(message, bytes):
            message = message.encode('utf-8')

        print("Sending -",message)

        try:
            self.socket.send(message)
        except socket.error:
            # attempt to reconnect if there was a connection error
            self.close()
            self.connect()
            self.socket.send(message)

    def listen(self, queue_names, function):
        self.subscribe({"queue_names":queue_names})

        while True:

            try:
                queue, message = self.get_message()
                function(queue, message)
            except socket.timeout:
                time.sleep(5)

    def send_message(self, instruction, payload):
        message = instruction + " "
        if isinstance(payload, dict):
            message += json.dumps(payload)

        if not isinstance(message, bytes):
            message = message.encode('utf-8')

        print("Sending -",message)

        try:
            self.socket.send(message)
        except socket.error:
            # attempt to reconnect if there was a connection error
            self.close()
            self.connect()
            self.socket.send(message)