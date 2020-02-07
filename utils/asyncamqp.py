from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode


import json
import asyncio


class AsyncAMQP:
    def __init__(self, loop: str, user: str, password: str, host: str, exchange_name: str, exchange_type: str):
        self.__loop = loop
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
                self.__cnx = await connect_robust(f"amqp://{self.__user}:{self.__password}@{self.__host}/", loop=self.__loop, timeout=5)
                self.__ch = await self.__cnx.channel()
                if self.__exchange_type == 'fanout':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.FANOUT, robust=True)
                elif self.__exchange_type == 'topic':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.TOPIC, robust=True)
                elif self.__exchange_type == 'direct':
                    self.__ex = await self.__ch.declare_exchange(self.__exchange_name, ExchangeType.DIRECT, robust=True)
                self.connected = True
                return self
            except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
                continue

    async def bind(self, queue_name: str, bindings: list, durable: bool):
        try:
            self.__q = await self.__ch.declare_queue(queue_name, durable=durable, arguments={'x-max-priority': 10}, robust=True)
            for b in bindings:
                await self.__q.bind(self.__ex, routing_key=b)
            return self
        except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
            await self.connect()

    async def send(self, data: object, persistent: bool, keys: list, priority: int):
        for k in keys:
            try:
                if persistent:
                    await self.__ex.publish(Message(body=json.dumps(data).encode(), delivery_mode=DeliveryMode.PERSISTENT, priority=priority), routing_key=k)
                else:
                    await self.__ex.publish(Message(body=json.dumps(data).encode(), delivery_mode=DeliveryMode.NOT_PERSISTENT, priority=priority), routing_key=k)
            except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
                await self.connect()

    async def receive(self):
        try:
            async with self.__q.iterator() as q:
                async for message in q:
                    async with message.process():
                        return json.loads(message.body.decode())
        except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
            await self.connect()
            await self.bind()

    async def cbreceive(self, cbfun, processor=None):
        try:
            async with self.__q.iterator() as q:
                async for message in q:
                    async with message.process():
                        asyncio.ensure_future(cbfun(message.body.decode()))
        except (ConnectionError, ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError, TimeoutError, RuntimeError):
            await self.connect()
            await self.bind()
