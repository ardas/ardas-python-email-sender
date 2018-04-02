import asyncio
import json
from time import sleep

import aio_pika
import pika
from pika.exceptions import ConnectionClosed, ChannelClosed, AMQPConnectionError
from aio_pika.robust_connection import RobustConnection, RobustChannel
from aio_pika.robust_queue import RobustQueue


class RabbitConnector:

    def __init__(self, config: dict, loop):
        self.loop = loop
        self._conn = None  # type: RobustConnection
        self._channel = None  # type: RobustChannel
        self.queue = None  # type: RobustQueue
        self.exchange = None  # type:
        self.consumer_tag = None  # type: str
        self._closed = False
        self.config = config
        self.message_count = 0
        self.credentials = pika.PlainCredentials(username=self.config['RMQ_LOGIN'],
                                                 password=self.config['RMQ_PASSWORD'])
        self.connection_parameters = pika.ConnectionParameters(host=self.config['RMQ_HOST'],
                                                               port=self.config['RMQ_PORT'],
                                                               credentials=self.credentials)

    async def _declare_channel(self):
        'Declare channel'
        return await self._conn.channel()

    async def _declare_connection(self):
        'Connect to RMQ'
        self._conn = await aio_pika.connect_robust(host=self.config['RMQ_HOST'],
                                                   port=self.config['RMQ_PORT'],
                                                   login=self.config['RMQ_LOGIN'],
                                                   password=self.config['RMQ_PASSWORD'],
                                                   loop=self.loop)

    async def declare_queue(self, queue):
        'Declare consumer queue'
        self.consumer_tag = queue
        return await self._channel.declare_queue(queue, durable=True)

    async def declare_exchange(self):
        await self._channel.declare_exchange(
            'direct', auto_delete=True
        )

    @staticmethod
    def close_callback(*args):
        'Callback when rabbitMQ connection closed'
        print('Connection closed')

    def connection_lost_callback(self, *args):
        'Callback when rabbitMQ connection lost. Try to reconnect'
        if self._closed:
            return
        self.check_connection()

    def reconnect_callback(self, *args):
        'Callback when rabbitMQ is reconnected'
        pass

    async def connect(self):
        'Initialize rabbitMQ connection'
        # check connection
        self.check_connection()
        # connect to RMQ
        await self._declare_connection()
        # add callbacks
        self._conn.add_close_callback(self.close_callback)
        self._conn.add_connection_lost_callback(self.connection_lost_callback)
        self._conn.add_reconnect_callback(self.reconnect_callback)
        # declare channel
        self._channel = await self._declare_channel()

    @staticmethod
    def on_open(connection):
        'Callback for check connection to rabbitMQ'
        connection.close()

    def check_connection(self):
        'Check connection to rabbitMQ. Reconnect if not connection'
        connection_exist = False
        while not connection_exist:
            try:
                connection = pika.SelectConnection(parameters=self.connection_parameters, on_open_callback=self.on_open)
                connection.ioloop.start()
                connection_exist = True
            except (ConnectionClosed, AMQPConnectionError) as ex:
                print('Cannot connect to rabbitMQ. Reason: {}. Reconnect in {} seconds'.format(
                    ex,
                    self.config['RECONNECT_INTERVAL']))
                sleep(int(self.config['RECONNECT_INTERVAL']))
                continue

    async def consume(self):
        'RabbitMQ consumer'
        print('[*] Waiting for messages. To exit press CTRL+C')
        # Reading message from queue
        async for message in self.queue:
            self.message_count += 1
            print("Get message - ", self.message_count)
            try:
                await self.message_process(message)
            except KeyboardInterrupt:
                break
            except ChannelClosed:
                pass
            except Exception as ex:
                message.reject(requeue=True)
                print(ex)

    async def message_process(self, message: aio_pika.IncomingMessage, func=print):
        'Message decode and feedback'
        with message.process(requeue=True):
            message_data = json.loads(message.body.decode())
            func(message_data)

    async def produce(self, data: dict, routing_key):
        'RabbitMQ producer'
        message = aio_pika.Message(body=json.dumps(data).encode(),
                                   content_type='application/json')

        await self._channel.default_exchange.publish(message,
                                                     routing_key=routing_key)
        return {'success': True}

    async def close(self):
        self._closed = True
        if self.queue:
            await self.queue.cancel(self.consumer_tag)
        await self._channel.close()
        await self._conn.close()
        print('Connection closed')
