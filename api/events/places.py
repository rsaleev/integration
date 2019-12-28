from threading import Thread
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from configuration import sys_log, wp_cnx, is_cnx, amqp_host, amqp_password, amqp_port, amqp_user
from aio_pika import connect, Message, ExchangeType, DeliveryMode, IncomingMessage
import json
from queue import Queue
from threading import Thread
import asyncio
import datetime
from dataclasses import dataclass


@dataclass
class PlacesListener:
    def __init__(self, modules_l):
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__amqp_receiver_cnx: object = None
        self.__amqp_receiver_ch: object = None
        self.__amqp_recevier_ex: object = None
        self.__amqp_receiver_q: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_sender_status = bool
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'PlacesListener'
        self.__trap_msg: dict = None
        self.__cmd_msg: dict = None
        self.__modules = modules_l

    @property
    def eventloop(self):
        return self.__loop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def trap_msg(self):
        return self.__trap_msg

    @trap_msg.setter
    def trap_msg(self, value):
        self.__trap_msg = value

    @property
    def trap_msg_ts(self):
        return self.__trap_msg_ts

    @trap_msg.setter
    def trap_msg_ts(self, value):
        self.__trap_msg_ts = value

    @property
    def cmd_msg(self):
        return self.__cmd_msg

    @cmd_msg.setter
    def cmd_msg(self, value):
        self.__cmd_msg = value

    @property
    def cmd_msg_ts(self):
        return self.__cmd_msg_ts

    @cmd_msg_ts.setter
    def cmd_msg_ts(self, value):
        self.__cmd_msg_ts = value

    @property
    def status(self):
        if self.__sql_status and self.__amqp_receiver_status and self.__amqp_sender_status:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _sql_connect(self):
        try:
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_is.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    async def _receiver_connect(self):
        try:
            await self.__logger.info('Establishing RabbitMQ connection')
            self.__amqp_receiver_cnx = await connect(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            self.__amqp_receiver_ch = await self.__amqp_receiver_cnx.channel()
            # connect to multiple exchanges
            # 1st exchange consists of queue with SNMP traps from barrier loops
            self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('statuses', ExchangeType.DIRECT)
            # 2nd exchnage consists of queue with AMPP commands events
            self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('commands', ExchangeType.DIRECT)
            # declare queue for
            self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('transits')
            await self.__amqp_receiver_q.bind(self.__amqp.receiver_ex, routing_key='loop')
            await self.__amqp_receiver_q.bind(self.__amqp.receiver_ex, routing_key='cmd')
            self.__amqp_receiver_status = True
            await self.__logger.info(f"RabbitMQ Connection:{self.__amqp_receiver_cnx}")
        except Exception as e:
            self.__amqp_receiver_status = False
            await self.__logger.error(e)
            raise
        finally:
            return self

    async def _sender_connect(self):
        try:
            asyncio.ensure_future(self.__logger.info('Establishing RabbitMQ connection'))
            while self.__amqp_sender_cnx is None:
                self.__amqp_sender_cnx = await connect(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            else:
                self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
                self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('places', ExchangeType.FANOUT)
                self.__amqp_sender_q = await self.__amqp_sender_ch.declare_queue('places')
                self.__amqp_sender_status = True
                asyncio.ensure_future(self.__logger.info(f"RabbitMQ Connection:{self.__amqp_sender_cnx}"))
        except Exception as e:
            self.__amqp_sender_status = False
            asyncio.ensure_future(self.__logger.error(e))
            raise
        finally:
            return self

    # callback to consume and store messages

    async def _receiver_on_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True, reject_on_redelivered=True):
            data = json.loads(message.body.decode())
            if message.info['routing_key'] == 'loop':
                self.trap_msg = data
            elif message.info['routing_key'] == 'cmd':
                self.cmd_msg = data
            message.ack()

    async def _amqp_receive(self):
        async with self.__amqp_receiver_q.iterator() as q:
            async for message in q:
                message.ack()

    async def _amqp_send(self, data: dict, key: str):
        body = json.dumps(data).encode()
        await self._amqp_sender_ex.publish(Message(body), routing_key=key, delivery_mode=DeliveryMode.PERSISTENT)

    async def _process(self):
        if self.__trap_msg and self.__cmd_msg:
            if (not datetime.now().timestamp() - self.__trap_msg['ts'] > 5
                and not datetime.now().timestamp() - self.__cmd_msg['ts'] > 5
                    and -2 < self.__trap_msg['ts'] - self.__cmd_msg['ts'] < 5):
                if self.__trap_msg['device_type'] == 1:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[None, 1, 1]))
                elif self.__trap_msg['device_type'] == 2:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[None, -1, 1]))
                self.trap_msg = None
                self.cmd_msg = None
        elif self.__trap_msg and not self.__cmd_msg:
            if not datetime.now().timestamp() - self.__trap_msg['ts'] > 2:
                if self.__trap_msg['device_type'] == 1:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[1, None, 1]))
                elif self.__trap_msg['device_type'] == 2:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[-1, None, 1]))
                self.trap_msg = None

    async def _distribute(self):
        places = await self.__dbconnector_is.callproc('is_places_get', rows=-1, values=[None])
        for p in places:
            asyncio.ensure_future(self._amqp_send(p))

    async def _dispatch(self):
        while True:
            await self._amqp_receive()
            await self._process()
            await self._distribute()

    def run(self):
        # try:
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._receiver_connect())
        self.eventloop.run_until_complete(asyncio.sleep(0.2))
        if self.status:
            self.eventloop.run_until_complete(self._dispatch())
        # except (KeyboardInterrupt, SystemExit):
        #     [task.cancel() for task in asyncio.Task.all_tasks() if not task.done()]
        #     # self.eventloop.run_in_executor(self.__dbconnector_is.pool.close())
        #     # self.eventloop.run_in_executor(self.__dbconnector_wp.pool.close())
        #     # asyncio.ensure_future(self.__dbconnector_is.pool.wait_closed())
        #     # asyncio.ensure_future(self.__dbconnector_wp.pool.wait_closed())
        #     # asyncio.ensure_future(self.__logger.shutdown())
        #     self.eventloop.run_until_complete(asyncio.sleep(0))
        #     self.eventloop.stop()
        #     self.eventloop.close()
