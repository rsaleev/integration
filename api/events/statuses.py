from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import json
from queue import Queue
import nest_asyncio
nest_asyncio.apply()


class StatusListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_receiver_status = bool
        self.name = 'StatusesListener'
        self.q = asyncio.Queue(maxsize=100, loop=asyncio.get_running_loop())

    @property
    def status(self):
        if self.__amqpconnector.connected and self.__dbconnector_is.connected:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        return self

    async def _amqp_connect(self):
        await self.__logger.info({"module": self.name, "info": "Establishing AMQP Connection"})
        self.__amqpconnector = await AsyncAMQP(loop=self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__amqpconnector.bind('statuses', ['#'], durable=False)
        asyncio.ensure_future(self.__logger.info({'module': self.name, 'info': 'AMQP Connection',
                                                  'status': self.__amqpconnector.connected}))
        return self

    async def _sql_connect(self):
        try:
            self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
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

    # callback for post-processing AMQP message
    async def _process(self, incoming_msg):
        data = json.loads(incoming_msg)
        await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value'], datetime.fromtimestamp(data['ts'])])

    # dispatcher
    async def _dispatch(self):
        while True:
            await self.__amqpconnector.cbreceive(self._process)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
        self.eventloop.run_forever()
