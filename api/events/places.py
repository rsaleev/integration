from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
from dataclasses import dataclass


@dataclass
class PlacesListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.name = 'PlacesListener'
        self.__trap: object = None
        self.__cmd: object = None

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
        if self.__sql_status and self.__amqp_connector.connected:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        return self

    async def _sql_connect(self):
        try:
            await self.__logger.info({'module': self.name, 'info': 'Establishing Integration RDBS Pool Connection'})
            self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
            await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
            self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_is.connected and self.__dbconnector_wp.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    async def _amqp_connect(self):
        asyncio.ensure_future(self.__logger.info({"module": self.name, "info": "Establishing RabbitMQ connection"}))
        self.__amqp_connector = await AsyncAMQP(loop=self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host,
                                                exchange_name='integration', exchange_type='topic').connect()
        await self.__amqp_connector.bind('places', ['status.loop2'])
        return self

    async def _dispatch(self):
        while True:
            try:
                msg = self.__amqp_connector.receive()
                await self.__logger.warning(msg)
                if msg['codename'] == 'BarrierLoop2Status':
                    self.__trap = msg
                elif msg['codename'] == 'PhyschalIn' or msg['codename'] == 'PhyschalOut' or msg['codename'] == 'OpenBarrier':
                    self.__cmd = msg
                if msg['codename'] in ['PhyschalIn', 'PhyschalOut'] and int(self.__cmd['ts']) - int(self.trap['ts']) < 10 and self.__trap['ampp_type'] == self.__cmd['device_type']:
                    if self.__trap['codename'] == 'PhyschalIn':
                        asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_challenged_upd', rows=0, values=[-1]))
                    elif self.__trap['codename'] == 'PhyschalOut':
                        asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_challenged_upd', rows=0, values=[1]))
                elif msg['codename'] == 'OpenBarrier' and self.__cmd['ts'] - int(self.trap['ts']) < 10 and self.__trap['ampp_type'] == self.__cmd['device_type']:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_commercial_upd', rows=0, values=[1]))
                elif int(datetime.now()) - self.__trap['ts'] < 10:
                    places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
                    for p in places:
                        await self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areFreePark'], None, p['areId']])
            except Exception as e:
                await self.__logger.error({'error': repr(e)})

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._dispatch())
