from typing import Callable

import aio_pika

from .heartbeat import HeartBeat


class Ahmq:
    """AlgoHero RabbitMQ"""
    _instance = None

    def __init__(self, url, heartbeat: HeartBeat):
        self.url = url
        self.connection = None
        self.channel = None
        self.heartbeat = heartbeat
        self.initialized = True  # singleton flag helper
        self.exchanges = {}
        self.queues = {}

        # register into beat
        self.heartbeat.register(self.check_connection_or_reconnect, interval=45)  # set interval to 45 sec/beat
        self.heartbeat.ticker()  # immediately start ticker

    def __new__(cls, *args, **kwargs):
        """Singleton helper"""
        if cls._instance is None:
            cls._instance = super(Ahmq, cls).__new__(cls)
        return cls._instance

    def _isNotConnected(self):
        return self.connection is None or self.connection.is_closed

    def _isConnected(self):
        return not (self.connection is None or self.connection.is_closed)

    async def connect(self):
        """Connect to RMQ."""
        if self._isNotConnected():
            try:
                self.connection = await aio_pika.connect(self.url)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
            except aio_pika.exceptions.AMQPException as e:
                raise
        return True

    async def check_connection_or_reconnect(self, **kwargs):
        """Check connection to RMQ. If not connected, then reconnect."""
        if self._isNotConnected():
            await self.connect()

    async def declare_or_get_exchange(self, exchange_name, exchange_type=aio_pika.ExchangeType.TOPIC):
        """Declare an exchange or get the existing."""
        if self._isNotConnected():
            await self.connect()
        if exchange_name not in self.exchanges:
            try:
                self.exchanges[exchange_name] = await self.channel.declare_exchange(exchange_name, type=exchange_type,
                                                                                    durable=True)
            except aio_pika.exceptions.AMQPException as e:
                raise
        return self.exchanges[exchange_name]

    async def _declare_or_get_queue(self, queue_name: str, auto_delete=True, arguments={}):
        """Declare a queue or get the existing.

        :param queue_name: queue name to be declared
        :param auto_delete: default to True; auto delete if queue is not used anymore
        :param arguments: additional arguments to be added in ``channel.declare_queue(arguments=arguments)``
        """
        if self._isNotConnected():
            await self.connect()
        if queue_name not in self.queues:
            try:
                self.queues[queue_name] = await self.channel.declare_queue(queue_name, auto_delete=auto_delete,
                                                                           durable=True, arguments=arguments)
            except aio_pika.exceptions.AMQPException as e:
                raise
        return self.queues[queue_name]

    async def bind_and_consume(self, callback: Callable, exchange_name: str, routing_key: str, queue_name: str):
        """Bind queue to exchange, then start consume a message in that queue.

        :param callback: callback function to be called when a message is arriving
        :param exchange_name: an exchange name
        :param queue_name: queue name
        :param routing_key: routing key
        """
        queue = await self._declare_or_get_queue(queue_name, auto_delete=True)
        await queue.bind(self.exchanges[exchange_name], routing_key=routing_key)
        await queue.consume(callback)

    async def publish(self, message: str, exchange_name: str, routing_key: str):
        """Publish a message to an exchange.

        Publish behavior really depends on the exchange:
            - For direct and topic exchanges: If no queue is bound with a matching routing key, the message is discarded.
            - For fanout exchanges: If no queues are bound to the exchange, the message is discarded.
            - For headers exchanges: If no queues match the message headers, the message is discarded.

        In the context of trading app, we will mostly use topic exchanges. As long as
        we bind queues to the exchange with the same routing key but different queue names,
        there will be no confusion among consumers.

        :param message: message to be published
        :param exchange_name: exchange name which message will be published into; must be initialized beforehand
        :param routing_key: a routing key which the queue has been bound to an exchange

        """
        if self._isNotConnected():
            await self.connect()
        await self.exchanges[exchange_name].publish(aio_pika.Message(body=message.encode()), routing_key=routing_key)

    @staticmethod
    def timely(start, end):
        return f"({round(end - start, 4)}s)"
