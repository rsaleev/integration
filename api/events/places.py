from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
from dataclasses import dataclass
from threading import Thread


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
        self.__logger = await AsyncLogger().getlogger(cfg.log)
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
        await self.__amqpconnector.bind('places', ['status.loop2', 'cmd.physchal.in', 'cmd.physchal.out'])
        return self

    async def _consume(self):
        while True:
            msg = await self.__amqpconnector.receive()
            if msg['codename'] == 'BarrierLoop2Status':
                self.trap = msg
                self.trap_set = True
            elif msg['codename'] in ['PhyschalIn', 'PhyschalOut']:
                self.cmd = msg
                self.cmd_set = True

    async def _process(self):
        while True:
            await asyncio.sleep(0.5)
            if self.trap_set and self.cmd_set and self.trap['amppId'] == self.cmd['amppId']:
                if self.cmd['codename'] == 'PhyschalIn':
                    await self.__dbconnector_is.callproc('is_places_challenged_upd', rows=0, values=[-1])
                    self.cmd_set = False
                    self.trap_set = False
                elif self.cmd['codename'] == 'PhyschalOut':
                    await self.__dbconnector_is.callproc('is_places_challenged_upd', rows=0, values=[1])
                    self.cmd_set = False
                    self.trap_set = False
            elif self.trap_set and not self.cmd_set:
                places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
                for p in places:
                    await self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areFreePark'], None, p['areId']])

    async def _dispatch(self):
        l1 = asyncio.get_event_loop()
        l2 = asyncio.get_event_loop()
        t1 = Thread(target=l1.run_until_complete(self._consume()))
        t1.start()
        t2 = Thread(target=l2.run_until_complete(self._process()))
        t2.start()

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
