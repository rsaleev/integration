from aio_pika import connect, ExchangeType, Message, DeliveryMode
from aio_pika.exceptions import ChannelClosed, ChannelNotFoundEntity, DeliveryError, PublishError, AMQPConnectionError, AuthenticationError, AMQPError, AMQPConnectionError
import json
import asyncio
from datetime import datetime


class AsyncAMQP:
    def __init__(self, user: str, password: str, host: str, exchange_name: str, exchange_type: str):
        self.__cnx = None
        self.__ch = None
        self.__ex = None
        self.__q = None
        self.__user = user
        self.__password = password
        self.__host = host
        self.__exchange_name = exchange_name
        self.__exchange_type = exchange_type
        self.connected = bool

    async def connect(self):
        while self.__cnx is None or self.__ch is None:
            try:
                self.__cnx = await connect(f"amqp://{self.__user}:{self.__password}@{self.__host}/", loop=asyncio.get_running_loop(), timeout=2)
                self.__ch = await self.__cnx.channel()
                if self.__exchange_type == 'fanout':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.FANOUT, passive=True, durable=True)
                elif self.__exchange_type == 'topic':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.TOPIC, passive=True, durable=True)
                elif self.__exchange_type == 'direct':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.DIRECT, passive=True, durable=True)
                self.connected = True
                return self
            except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
                await asyncio.sleep(0.2)
                continue

    async def disconnect(self):
        await self.__cnx.close()

    async def bind(self, queue_name: str, bindings: list, durable: bool):
        try:
            self.__q = await self.__ch.declare_queue(name=queue_name, arguments={'x-max-priority': 10}, auto_delete=False if durable else True, durable=durable)
            for b in bindings:
                await self.__q.bind(self.__ex, routing_key=b)
            return self
        except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
            await asyncio.sleep(0.2)
            raise
            await self.connect()

    async def send(self, data: dict, persistent: bool, keys: list, priority: int):
        for k in keys:
            try:
                await self.__ex.publish(Message(body=json.dumps(data, default=str).encode(), delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT, priority=priority), routing_key=k)
            except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
                raise
                # await self.connect()

    async def cbreceive(self, callback,  reject_redelivered=bool):
        try:
            async with self.__q.iterator() as q:
                async for message in q:
                    async with message.process(requeue=True, reject_on_redelivered=reject_redelivered):
                        redelivered = message.info()['redelivered']
                        key = message.info()['routing_key']
                        data = json.loads(message.body.decode())
                        await callback(redelivered, key, data)
        except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
            raise
            await self.connect()
