from dotenv import load_dotenv, find_dotenv
import dsnparse
from datetime import datetime
import os
import pika
import json
from database.database import database_insert

class SaleReportsConsumer:
    exchange = 'sale_report_message'
    parameters = None
    prefetch_count = 30

    def __init__(self):
        load_dotenv(find_dotenv())

    def callback(self, ch, method_frame, _header_frame, body):
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' Processing...')

        self.execute(ch, method_frame, _header_frame, body)

        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' Processed')

        ch.basic_ack(delivery_tag=method_frame.delivery_tag)

    def execute(self, ch, method_frame, _header_frame, body):
        _body = json.loads(body)
        print(_body)
        database_insert('sale_report', _body)
        return None

    def start(self):
        while True:
            try:
                dsn = os.getenv("RABBITMQ_DSN")

                r = dsnparse.parse(dsn)

                credentials = pika.PlainCredentials(r.user, r.password)

                parameters = pika.ConnectionParameters(r.host, r.port, '/', credentials)

                connection = pika.BlockingConnection(parameters)

                queue_name = self.exchange
                exchange_name = queue_name

                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
                channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='')
                channel.basic_qos(prefetch_count=int(self.prefetch_count))
                channel.basic_consume(queue_name, self.callback)
                channel.start_consuming()
            except:
                raise



if __name__ == "__main__":
    SaleReportsConsumer().start()
