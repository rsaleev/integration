from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import json
import configuration as cfg


class MoneyListener:

    def __init__(self, devices_l):
        self.__dbconnector_is: object = None
        self.__dbconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__devices = devices_l
        self.name = 'MoneyListener'

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

    async def _process_money(self, data: list):
        for d in data:
            asyncio.ensure_future(self.__dbconnector_is.callproc('is_money_upd', rows=0, values=[data['curTerId'], data['curChannelId'], data['curValue'], data['curQty']]))

    

    async def _dispatch(self):
        while True:
            try:
                money = await self.__dbconnector_wp.callproc('wp_money_get', rows=0, values=[])
                await self._process_money(money)
                inventories = await self.__dbconnector_wp.callproc('wp_inventory_get', rows=0, values=[None])
                await self._process_inventory(inventories)
            except Exception as e:
                asyncio.ensure_future(self.__logger.error(repr(e)))
                continue
            else:
                await asyncio.sleep(cfg.rdbs_polling_interval)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
        self.eventloop.run_forever()
