from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
from dataclasses import dataclass


@dataclass
class PlacesListener:
    def __init__(self, devices_l):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__amqp_connector_t1: object = None
        self.__amqp_connector_t2: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.name = 'PlacesListener'
        self.__trap: object = None
        self.__cmd: object = None
        self.__devices = devices_l

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
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
            self.__dbconnector_wp = await AsyncDBPool(conn=wp_cnx, loop=self.eventloop).connect()
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
        await self.__amqp_connector.bind('places', ['command.phychal.*', 'status.loop2'])
        asyncio.ensure_future(self.__logger.info({"module": self.name, "info": "RabbitMQ connection",
                                                  "status": True if self.__amqp_connector_1.conncted and self.__amqp_connector_2.connected else False}))
        return self

    async def _dispatch(self):
        while True:
            # receive trap message
            self.trap = await self.__amqp_connector_t1.receive()
            # receive command message
            self.cmd = await self.__amqp_connector_t2.receive()
            if not trap is None and not cmd is None and trap['value'] == 1 and cmd['value'] == 101:
                area = next(d['areaId'] for d in self.__devices if d['amppId'] == cmd['device_id'])
                self.trap = None
                self.cmd = None
                asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_transit_upd', [None, 1, area]))
            elif not trap is None and not cmd is None and trap['value'] == 1 and cmd['value'] == 102:
                area = next(d['areaId'] for d in self.__devices if d['amppId'] == cmd['device_id'])
                self.trap = None
                self.cmd = None
                asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_transit_upd', [None, -1, area]))
            elif trap is None and not cmd is None and cmd['value'] == 103:
                area = next(d['areaId'] for d in self.__devices if d['amppId'] == cmd['device_id'])
                self.cmd = None
                asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_transit_upd', [None, -1, area]))
            elif trap is None and not cmd is None and cmd['value'] in [101, 102]:
                pass
            elif not trap is None and cmd is None and trap['value'] == 1:
                area = next(d['areaId'] for d in self.__devices if d['terAddress'] == trap['device_id'])
                self.trap = None
                if trap['device_type'] == 1:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_transit_upd', [None, 1, area]))
                elif trap['device_type'] == 2:
                    asyncio.ensure_future(self.__dbconnector_is.callproc('is_places_transits_upd', [None, -1, area]))

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._receiver_connect())
        if self.status:
            self.eventloop.run_until_complete(self._dispatch())
