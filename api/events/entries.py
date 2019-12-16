from datetime import datetime
import asyncio
from dataclasses import dataclass
import aio_pika
from aio_pika import robust_connection, Message, ExchangeType, DeliveryMode, IncomingMessage
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from config.configuration import sql_conn, sql_pool
import json

@dataclass
class TransitEventListener:

    def __init__(self):
        self._amqp_sender_cnx: object = None
        self._amqp_sender_ch: object = None
        self._amqp_sender_status = bool
        self._amqp_reciever_cnx: object = None
        self._amqp_reciever_ch: object = None
        self._amqp_recevier_status = bool
        self._sql_connection = bool
        self.__logger: object = None
        self.__loop: object = None

    @property
    def loop(self):
        return self.__loop

    @loop.setter
    def loop(self, value):
        self.__loop = value

    @loop.getter
    def loop(self):
        return self.__loop

    @property
    def sender(self):
        return self._amqp_sender_status

    @property
    def receiver(self):
        return self._amqp_receiver_status

    async def _sender_connect(self, loop):
        try:
            self._amqp_sender_cnx = await robust_connection.connect(host='192.168.1.3', login='guest', password='guest', loop=loop)
            self._amqp_sender_ch = await self._amqp_cnx.channel()
            self._aqmp_sender_ex = await self._amqp_sender_ch.declare_exchange('entries', ExchangeType.FANOUT)
            self._aqmp_sender_q = await self._amqp_sender_ch.declare_queue(exclusive=True)
            self._amqp_sender_status = True

    async def _receiver_connect(self, loop):
        try:
            self._amqp_receiver_cn = await robust_connection.connect(host='192.168.1.3', login='guest', password='guest', loop=loop)
            self._amqp_receiver_ch = await self._amqp_reciver_cn.channel()
            self._amqp_receiver_ex = await self._amqp_reciever_ch.declare_exchange('loop_traps')
            self._amqp_receiver_q = await self._amqp_receiver_ch.declare_queue(exclusive=True)
            await self._amqp_reciver_q.bind(self._amqp.receiver_ex)
            self._amqp_sender_status = True
            return self
        except:
            self._amqp_sender_status = False
            raise

    async def _receiver_on_message(self, message: IncomingMessage):
        with message.process():
            message = message.body
            return message

    async def _sql_connect(self):
        self._db_connector = await AsyncDBPool(size=5, name='transit_listener', conn=sql_conn, loop=loop).connect()
        return self

    async def _logger_init(self, loop):
        self.__logger = await AsyncLogger().getlogger('test.log')
        return self

    async def _dispatch(self):
        try:
            message = await self._amqp_receiver_q.consume(self._receiver_on_message)
            if message['oid'] == (1, 3, 6, 1, 4, 1, 40383, 1, 2, 2, 113) and message['value'] == 1:
                message_ts = datetime.now().timestamp()
                records = await self._sql_connection.callproc('entry_integration_get', [message_ts])
                for record in records:
                    message = json.dumps(record).encode()
                    await self._amqp_seexchange.publish(message=Message(message, delivery_mode=DeliveryMode.PERSISTENT), routing_key='entry')
        except Exception as e:
            await self.__logger.error(e)

    def start(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self._logger_init())
        self.loop.run_until_complete(self._sql_connect())
        self.loop.run_until_complete(self._receiver_connect())
        self.loop.run_until_complete(self._sender_connect())
        self.loop.run_forever(self._dispatch())
