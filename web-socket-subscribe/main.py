import asyncio
import websockets
import os
from quixstreams import Application
import json
import aiohttp
import ssl

from dotenv import load_dotenv
load_dotenv()


class WebSocketPublisher:
    def __init__(self, app, topics, producer):
        self.app = app
        self.topics = topics
        self._producer = producer

    async def publish_messages(self, websocket, path):
        print(f"Client connected to socket. Path={path}")
        path_parts = path.strip('/').split('/')
        topic_name = path_parts[1]
        stream_id = path_parts[3]

        if topic_name not in self.topics:
            my_topic = self.app.topic(name=topic_name)
            self.topics[topic_name] = my_topic
        try:
            async for message in websocket:
                # Here you handle incoming messages and publish them to the topic
                print(f"Publishing message to {topic_name}: {message}")
                self._producer.produce(
                    topic=self.topics[topic_name].name,
                    key=stream_id,
                    value=message.encode('utf-8'))
        except websockets.exceptions.ConnectionClosedOK:
            print(f"Publisher {path} disconnected normally.")
        except websockets.exceptions.ConnectionClosed as e:
            print(f"Publisher {path} disconnected with error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            print(f"Publisher {path} has stopped publishing.")

    async def start_publisher_server(self):
        print("Starting publisher server...")
        server = await websockets.serve(self.publish_messages, '0.0.0.0', 8081)
        await server.wait_closed()


class WebSocketSubscriber:
    def __init__(self, app, topics, consumers, websocket_connections):
        self.app = app
        self.topics = topics
        self.consumers = consumers
        self.websocket_connections = websocket_connections

    async def consume_messages(self, topic_name):
        consumer = self.consumers[topic_name]
        while True:
            message = consumer.poll(1)
            if message is not None:
                value = bytes.decode(message.value())
                # Assuming 'V' is the key in the value dict that holds the message content
                if topic_name in self.websocket_connections:
                    for client in self.websocket_connections[topic_name]:
                        try:
                            await client.send(json.dumps(value))
                        except websockets.exceptions.ConnectionClosed:
                            print("Connection already closed.")
                print(value)
                print(f"Sent to subscribers of {topic_name}.")
            else:
                await asyncio.sleep(1)

    async def subscribe_messages(self, websocket, path):
        print(f"Client connected to socket. Path={path}")
        path_parts = path.strip('/').split('/')
        # todo update index??
        topic_name = path_parts[1] 

        if topic_name not in self.topics:
            my_topic = self.app.topic(name=topic_name)
            self.topics[topic_name] = my_topic
            self.consumers[topic_name] = self.app.get_consumer()
            self.consumers[topic_name].subscribe([my_topic.name])
            # Start consuming messages for this topic
            asyncio.create_task(self.consume_messages(topic_name))

        if topic_name not in self.websocket_connections:
            self.websocket_connections[topic_name] = []
        self.websocket_connections[topic_name].append(websocket)
        print(f'There are {len(self.websocket_connections[topic_name])} subscribers')

        try:
            await websocket.wait_closed()
        except websockets.exceptions.ConnectionClosedOK:
            print(f"Client {path} disconnected normally.")
        except websockets.exceptions.ConnectionClosed as e:
            print(f"Client {path} disconnected with error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            print("Removing client from connection list")
            if path in self.websocket_connections:
                self.websocket_connections[path].remove(websocket)


    async def start_subscriber_server(self):
        print("Starting subscriber server...")
        server = await websockets.serve(self.subscribe_messages, '0.0.0.0', 8081)
        await server.wait_closed()


async def main():
    app = Application.Quix("heatmap-web-sockets-v1", auto_offset_reset="latest")
    producer = app.get_producer()
    topics = {}
    consumers = {}
    websocket_connections = {}

    # publisher = WebSocketPublisher(app, topics, producer)
    subscriber = WebSocketSubscriber(app, topics, consumers, websocket_connections)

    await asyncio.gather(
        # publisher.start_publisher_server(),
        subscriber.start_subscriber_server()
    )

# Run the application with exception handling
try:
    asyncio.run(main())
except Exception as e:
    print(f"An error occurred: {e}")