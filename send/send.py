import pika
import time
import json
import threading
import signal
import random
from datetime import datetime


class MainApp:
    host = 'rabbitmq'

    def __init__(self):
        self.stop_event = threading.Event()
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.commands_thread = threading.Thread(target=self.send_commands)
        self.data_thread = threading.Thread(target=self.send_data)

    def run(self):
        self.commands_thread.start()
        self.data_thread.start()
        self.commands_thread.join()
        self.data_thread.join()
        print('Exited')

    def exit_gracefully(self, *args, **kwargs):
        self.stop_event.set()
        print('Exiting')

    def send_commands(self):
        with pika.BlockingConnection(pika.ConnectionParameters(host=self.host)) as connection:
            channel = connection.channel()

            channel.queue_declare(queue='commands')
            while not self.stop_event.is_set():
                value = {
                    'timestamp': datetime.now().timestamp(),
                    'value': random.random(),
                }
                channel.basic_publish(
                    exchange='',
                    routing_key='commands',
                    body=json.dumps(value),
                )
                print(f'Sent {value}')
                time.sleep(2)
            print('Send commands finish')

    def send_data(self):
        with pika.BlockingConnection(pika.ConnectionParameters(host=self.host)) as connection:
            channel = connection.channel()

            channel.queue_declare(queue='data')
            value = {
                'lat': 0,
                'lon': 0,
            }

            while not self.stop_event.is_set():
                value['lat'] += random.uniform(-1, 1)
                value['lon'] += random.uniform(-1, 1)
                channel.basic_publish(
                    exchange='',
                    routing_key='data',
                    body=json.dumps(value),
                )
                print(f'Sent {value}')
                time.sleep(1)
            print('Send data finish')


if __name__ == '__main__':
    MainApp().run()
