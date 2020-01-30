import configuration as cfg
import json
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
import subprocess
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncamqp import AsyncAMQP


class AsyncPingPoller:
    def __init__(self, devices_l):
        self.__amqp_connector = None
        self.__eventloop = None
        self.__logger = None
        self.name = 'PingPoller'
        self.__devices = devices_l
        self.__amqp_status = False

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @dataclass
    class NetworkStatus:
        def __init__(self):
            self.codename = 'Network'
            self.__value: bool = None
            self.__device_id: int = None
            self.__device_ip: str = None
            self.__device_type: int = None
            self.__ampp_id: int = None
            self.__ampp_type: int = None
            self.__ts = datetime

        @property
        def device_id(self):
            return self.__device_id

        @device_id.setter
        def device_id(self, value: int):
            self.__device_id = value

        @device_id.getter
        def device_id(self):
            return self.__device_id

        @property
        def device_type(self):
            return self.__device_type

        @device_type.setter
        def device_type(self, value: int):
            self.__device_type = value

        @device_type.getter
        def device_type(self):
            return self.__device_type

        @property
        def device_ip(self):
            return self.__device_ip

        @device_ip.setter
        def device_ip(self, value: str):
            self.__device_ip = value

        @device_ip.getter
        def device_ip(self):
            return self.__device_ip

        @property
        def ampp_id(self):
            return self.__ampp_id

        @ampp_id.setter
        def ampp_id(self, value):
            self.__ampp_id = value

        @ampp_id.getter
        def ampp_id(self):
            return self.__ampp_id

        @property
        def ampp_type(self):
            return self.__ampp_type

        @ampp_type.setter
        def ampp_type(self, value):
            self.__ampp_type = value

        @ampp_type.getter
        def ampp_type(self):
            return self.__ampp_type

        @property
        def value(self):
            return self.__value

        @value.setter
        def value(self, v: bool):
            self.__value = value

        @value.getter
        def value(self):
            if self.__value:
                return 'ONLINE'
            else:
                return 'OFFLINE'

        @property
        def ts(self):
            return self.__ts

        @ts.setter
        def ts(self, value: datetime):
            self.__ts = value

        @ts.getter
        def ts(self):
            return self.__ts

        @property
        def data(self):
            return {'device_id': self.device_id,
                    'device_type': self.device_type,
                    'codename': self.codename,
                    'value': self.value,
                    'ts': self.ts,
                    'ampp_id': self.ampp_id,
                    'ampp_type': self.ampp_type,
                    'device_ip': self.device_ip}

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        return self

    async def _amqp_connect(self):
        asyncio.ensure_future(self.__logger.info({"module": self.name, "info": "Establishing RabbitMQ connection"}))
        self.__amqp_connector = await AsyncAMQP(loop=self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        asyncio.ensure_future(self.__logger.info({"module": self.name, "info": "RabbitMQ connection", "status": self.__amqp_connector.connected}))
        return self.__amqp_connector

    def _ping(self, hostname):
        try:
            subprocess.check_output(["ping", "-c", "1", "-W", "1", hostname])
            return True
        except subprocess.CalledProcessError:
            return False

    async def _dispatch(self):
        while True:
            for device in self.__devices:
                ping_object = self.NetworkStatus()
                ping_object.device_id = device['terId']
                ping_object.device_type = device['terType']
                ping_object.device_ip = device['terIp']
                ping_object.ampp_id = device['amppId']
                ping_object.ampp_type = device['amppType']
                ping_object.ts = datetime.now().timestamp()
                status = await self.eventloop.run_in_executor(None, self._ping, device['terIp'])
                await self.__amqp_connector.send(ping_object.data, persistent=True, keys=['status.online'], priority=1)
            await asyncio.sleep(cfg.snmp_polling)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
