from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
import aio_pika
from aio_pika import Message, ExchangeType, DeliveryMode, IncomingMessage, connect
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json


class StatusListener:

    def __init__(self):
        self.__amqp_receiver_cnx: object = None
        self.__amqp_receiver_ch: object = None
        self.__amqp_recevier_ex: object = None
        self.__amqp_receiver_q: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'StatusesListener'

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _receiver_connect(self):
        try:
            self.__amqp_receiver_cnx = await connect(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            self.__amqp_receiver_ch = await self.__amqp_receiver_cnx.channel()
            self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('statuses')
            self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('status')
            # await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='loop')
            # await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='payment')
            # await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='device')
            await self.__logger.info(f"Connected to:{self.__amqp_receiver_cnx}")
            self.__amqp_receiver_status = True
            return self
        except Exception as e:
            self.__amqp_receiver_status = False
            await self.__logger.error(e)
            raise
        finally:
            return self

    async def _receiver_on_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True, reject_on_redelivered=True):
            pass

    async def _sql_connect(self):
        try:
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_is.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
            return self
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    async def _dispatch(self):
        while True:
            try:
                async with self.__amqp_receiver_q.iterator() as q:
                    async for message in q:
                        message.ack()
                        data = json.loads(message.body.decode())
                        asyncio.ensure_future(self.__logger.debug(data))
                        await self.__dbconnector_is.callproc('wp_status_ins', rows=0, values=[data['device_id'], data['device_type'], data['ampp_id'], data['ampp_type'],
                                                                                              data['codename'], data['value'], data['device_ip'], datetime.fromtimestamp(data['ts'])])
            except Exception as e:
                asyncio.ensure_future(self.__logger.error(e))
                continue

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._sql_connect())
            self.eventloop.run_until_complete(self._receiver_connect())
            self.eventloop.run_until_complete(self._dispatch())
            # self.eventloop.run_forever()
        except KeyboardInterrupt:
            [task.cancel() for task in asyncio.Task.all_tasks() if not task.done()]
            # self.eventloop.stop()
            self.eventloop.stop()
