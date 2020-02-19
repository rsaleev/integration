
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
import json


class ParkingListener:
    def __init__(self, devices_l):
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.name = 'ParkingListener'
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

    class PlacesWarning:
        def __init__(self,  value):
            self.codename = 'Parking'
            self.value = value
            self.__device_id: int = next(d['terId'] for d in self.__devices if d['terType'] == 0)
            self.__device_address: int = next(d['terAddress'] for d in self.__devices if d['terType'] == 0)
            self.__device_ip: str = next(d['terIp'] for d in self.__devices if d['terType'] == 0)
            self.__device_type: int = 0
            self.__ampp_id: int = next(d['amppId'] for d in self.__devices if d['terType'] == 0)
            self.__ampp_type: int = 1
            self.__ts = datetime.now()

        @property
        def data(self):
            return {'device_id': self.__device_id,
                    'device_address': self.__device_address,
                    'device_type': self.__device_type,
                    'codename': self.__codename,
                    'value': self.__value,
                    'ts': self.__ts,
                    'ampp_id': self.__ampp_id,
                    'ampp_type': self.__ampp_type,
                    'device_ip': self.__device_ip}

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger('places.log')
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Integration RDBS Pool Connection'})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection'})
        self.__amqpconnector = await AsyncAMQP(None, cfg.amqp_user, cfg.amqp_password, cfg.amqp_host, 'integration', 'topic').connect()
        return self

    async def _process(self, data):
        for d in data:
            if d['areaId'] == 1 and d['commFree'] == 0:
                parking = self.PlacesWarning('COMM_FULL')
                await self.__amqpconnector.send(parking.data, persistent=True, keys=['status'], priority=9)
            if d['areId'] == 1 and d['physchalFree'] == 0:
                parking = self.PlacesWarning('PHYSCHAL_FULL')
                await self.__amqpconnector.send(parking.data, persistent=True, keys=['status'], priority=9)

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
