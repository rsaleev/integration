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
        self.name = 'StatusesListener'

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        await self.__logger.info({"module": self.name, "info": "Establishing AMQP Connection"})
        self.__amqpconnector = await AsyncAMQP(loop=self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__amqpconnector.bind('statuses', ['status.*', 'command.*'], durable=False)
        asyncio.ensure_future(self.__logger.info({'module': self.name, 'info': 'AMQP Connection',
                                                  'status': self.__amqpconnector.connected}))
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        return self

    # callback for post-processing AMQP message
    async def _process(self, incoming_msg):
        data = json.loads(incoming_msg)
        await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value'], datetime.fromtimestamp(data['ts'])])
        await asyncio.sleep(0.5)

    # dispatcher
    async def _dispatch(self):
        while True:
            await self.__amqpconnector.cbreceive(self._process)
            await asyncio.sleep(0.5)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
        self.eventloop.run_forever()
