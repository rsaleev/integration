from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import json
import configuration as cfg


class InventoryListener:

    def __init__(self, devices_l):
        self.__dbconnector_is: object = None
        self.__dbconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__devices = devices_l
        self.name = 'CashboxListener'

        class InventoryWarning:
            def __init__(self, device_id, device_address, device_ip, device_type, ampp_id, ampp_type):
                self.codename = 'Cashbox'
                self.value = 'ALMOST_FULL'
                self.__device_id: int = None
                self.__device_address: int = None
                self.__device_ip: str = None
                self.__device_type: int = None
                self.__ampp_id: int = None
                self.__ampp_type: int = None
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

    # primary initialization of logging and connections
    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        await self.__logger.info({"module": self.name, "info": "Establishing AMQP Connection"})
        await self.__logger.info({"module": self.name, "info": "Establishing RDBS Integration Connection"})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        asyncio.ensure_future(self.__logger.info({'module': self.name, 'info': 'RDBS Integration Connection',
                                                  'status': self.__dbconnector_is.connected}))
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
        await self.__logger.info({"module": self.name, "info": "Establishing RDBS Wisepark Connection"})
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
        asyncio.ensure_future(self.__logger.info({'module': self.name, 'info': 'RDBS Wisepark Connection',
                                                  'status': self.__dbconnector_wp.connected}))

    async def _process_inventory(self, data: list):
        for d in data:
            if d['storageCapacity'] == d['storageLimit']:
                self.__amqpconnector.send()
            self.__dbconnector_is.callproc('is_inventory_upd', rows=0, values=[data['curTerId'], data['curChannelId'], data['curTotal']])
