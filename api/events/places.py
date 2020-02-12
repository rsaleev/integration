from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
from dataclasses import dataclass
from threading import Thread
import json


@dataclass
class PlacesListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.name = 'PlacesListener'
        self.cmd = None
        self.cmd_set = False
        self.trap = None
        self.trap_set = False

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
    def status(self):
        if self.__sql_status and self.__amqp_connector.connected:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger('places.log')
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        return self

    async def _sql_connect(self):
        await self.__logger.info({'module': self.name, 'info': 'Establishing Integration RDBS Pool Connection'})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
        return self

    async def _amqp_connect(self):
        asyncio.ensure_future(self.__logger.info({"module": self.name, "info": "Establishing RabbitMQ connection"}))
        self.__amqpconnector = await AsyncAMQP(loop=self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host,
                                               exchange_name='integration', exchange_type='topic').connect()
        await self.__amqpconnector.bind('places', bindings=['status.loop2', 'command.physchal.in', 'command.physchal.out'], durable=False)
        return self

    async def _process(self, incoming_msg):
        data = json.loads(incoming_msg)
        if data['codename'] == 'BarrierLoop2Status' and data['value'] == "OCCUPIED":
            self.status = data
            self.status_set = True
            return self
        elif data['codename'] in ['PhyschalIn', 'PhyschalOut']:
            self.cmd = data
            self.cmd_set = True
            return self

    async def _dispatch(self):
        while True:
            places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
            for p in places:
                await self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areFreePark'], None, p['areId']])
            await asyncio.sleep(3)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
