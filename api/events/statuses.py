from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
import aio_pika
from aio_pika import Message, ExchangeType, DeliveryMode, IncomingMessage, connect_robust
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json


class StatusListener:

    def __init__(self):
        self.__amqp_cnx: object = None
        self.__amqp_ch: object = None
        self.__amqp_ex: object = None
        self.__amqp_q: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'StatusesListener'

    @property
    def status(self):
        if self.__amqp_receiver_status and self.__sql_status:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _amqp_connect(self):
        try:
            self.__amqp_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            self.__amqp_ch = await self.__amqp_cnx.channel()
            self.__amqp_ex = await self.__amqp_ch.declare_exchange('integration', ExchangeType.TOPIC)
            self.__amqp_q = await self.__amqp_ch.declare_queue('status', durable=True)
            await self.__amqp_q.bind(self.__amqp_ex, '#')
            await self.__logger.info(f"Connected to:{self.__amqp_cnx}")
            self.__amqp_receiver_status = True
            return self
        except Exception as e:
            self.__amqp_receiver_status = False
            await self.__logger.error(e)
            raise
        finally:
            return self

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
                async with self.__amqp_q.iterator() as q:
                    async for message in q:
                        message.ack()
                        data = json.loads(message.body.decode())
                        asyncio.ensure_future(self.__logger.debug(data))
                        await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value']])
            except Exception as e:
                asyncio.ensure_future(self.__logger.error(e))
                continue

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
