import asyncio
import json
from .queues import Topics
import time

class Connections(object):
    connections = []
    

class ServerProtocol(asyncio.Protocol):
    def __init__(self, loop=None):
        super().__init__()
        self.loop = loop or asyncio.get_event_loop()
        Connections.connections.append(self)
        
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, message):
        message = message.decode()
        print('Data received: {!r}'.format(message))
        instruction, data = message.split(' ', 1)
        data = json.loads(data)
        asyncio.create_task(self.execute_instruction(instruction, data))

    @asyncio.coroutine
    def execute_instruction(self, instruction, data):
        if instruction == "create_queue":
            self.create_queue(data)

        elif instruction == "publish":
            self.publish(data)

        elif instruction == "subscribe":
            self.subscribe(data)

    def send_message(self, queue, payload):
        message = queue + " "
        if isinstance(payload, (str, int)):
            payload = {"payload":payload}

        if isinstance(payload, dict):
            message += json.dumps(payload)

        if not isinstance(message, bytes):
            message = message.encode('utf-8')

        print("Sending -",message)

        try:
            self.transport.write(message)
        except Exception:
            self.transport.close()

    def start_subscription(self, queue_names):
        for queue_name in queue_names:
            try:
                payload = Topics.dequeue(queue_name)
                if payload is not None:
                    self.send_message(queue_name, payload)
            except:
                continue
        self.loop.call_later(2, self.start_subscription, queue_names)

    def subscribe(self, data):
        queue_names = data.get("queue_names", None)
        print("Sending data from the -",queue_names)
        self.start_subscription(queue_names)

    def create_queue(self, data):
        queue_names = data.get("queue_names", None)

        if type(queue_names) is str:
            queue_names = [queue_names]

        for queue_name in queue_names:
            try:
                Topics.add(queue_name)
            except:
                continue
            
    def publish(self, data):
        queue_names = data.get("queue_names", None)
        payload = data.get("payload", None)

        if type(queue_names) is str:
            queue_names = [queue_names]

        for queue_name in queue_names:
            try:
                Topics.enqueue(queue_name, payload)
            except:
                continue
            

def main():

    loop = asyncio.get_event_loop()
    server = loop.create_server(lambda: ServerProtocol(), "127.0.0.1", 8888)
    loop.run_until_complete(server)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == '__main__':
    main()