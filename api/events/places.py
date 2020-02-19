from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
import json


class PlacesListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.name = 'PlacesListener'

    @property
    def eventloop(self):
        return self.__loop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger('places.log')
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Integration RDBS Pool Connection'})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
        return self

  
    async def _process(self, data):
        for d in data:
            asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[d['areFreePark'], None, d['areId']]))

    async def _dispatch(self):
        while True:
            try:
                places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
                await self._process(places)
            except Exception as e:
                asyncio.ensure_future(self.__logger.error({'module': self.name, 'error': repr(e)}))
                continue
            else:
                await asyncio.sleep(cfg.rdbs_polling_interval)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
        self.eventloop.run_forever()
