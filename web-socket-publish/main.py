import asyncio
import websockets
from quixstreams import Application

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
                print(f"Publishing message to {topic_name}")
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
        server = await websockets.serve(self.publish_messages, '0.0.0.0', 80)
        await server.wait_closed()


async def main():
    app = Application.Quix()
    producer = app.get_producer()
    topics = {}
    publisher = WebSocketPublisher(app, topics, producer)

    await asyncio.gather(
        publisher.start_publisher_server(),
    )

# Run the application with exception handling
try:
    asyncio.run(main())
except Exception as e:
    print(f"An error occurred: {e}")