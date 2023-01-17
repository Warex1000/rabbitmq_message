import pika
import json
import csv


def main():
    with pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq')) as connection:
        channel = connection.channel()

        channel.queue_declare(queue='data')
        channel.queue_declare(queue='commands')

        def receive_data(ch, method, properties, body):
            body = body.decode()
            print(f'Received data {body}')

            with open('data.csv', 'a') as f:
                csvwriter = csv.writer(f)
                csvwriter.writerow(json.loads(body).values())

        def receive_commands(ch, method, properties, body):
            body = body.decode()
            print(f'Received command {body}')

            with open('commands.csv', 'a') as f:
                csvwriter = csv.writer(f)
                csvwriter.writerow(json.loads(body).values())

        channel.basic_consume(queue='commands', on_message_callback=receive_commands, auto_ack=True)
        channel.basic_consume(queue='data', on_message_callback=receive_data, auto_ack=True)
        channel.start_consuming()


if __name__ == '__main__':
    main()

