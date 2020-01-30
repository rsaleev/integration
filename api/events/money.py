from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json


class MoneyListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__dbconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_receiver_status = bool
        self.name = 'MoneyListener'

    @property
    def status(self):
        if self.__amqpconnector.connected and self.__dbconnector_is.connected:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        return self

    async def _amqp_connect(self):
        await self.__logger.info({"module": self.name, "info": "Establishing AMQP Connection"})
        self.__amqpconnector = await AsyncAMQP(loop=self.eventloop,
                                               user=amqp_user,
                                               password=amqp_password,
                                               host=amqp_host,
                                               exchange_name='integration',
                                               exchange_type='topic',
                                               queue_name='statuses',
                                               priority_queue=True,
                                               binding='#').connect()
        asyncio.ensure_future(self.__logger.info({'module': self.name, 'info': 'AMQP Connection',
                                                  'status': self.__amqpconnector.connected}))
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
                data = await self.__amqpconnector.receive()
                asyncio.ensure_future(self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value'], datetime.fromtimestamp(data['ts'])]))
            except Exception as e:
                asyncio.ensure_future(self.__logger.error(e))
                continue
