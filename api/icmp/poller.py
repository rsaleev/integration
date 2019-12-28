from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode
from configuration import amqp_host, amqp_password, amqp_user, sys_log, snmp_polling
import json
import asyncio
import os
from utils.asyncsql import AsyncDBPool
from dataclasses import dataclass
from datetime import datetime
import subprocess
from utils.asynclog import AsyncLogger


class AsyncPingPoller:
    def __init__(self, devices_l):
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.name = 'PingPoller'
        self.__devices = devices_l
        self.__logger = None

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
        def value_(self):
            return self.__value

        @value_.setter
        def value_(self, value: bool):
            self.__value = value

        @value_.getter
        def value_(self):
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
                    'value': self.value_,
                    'ts': self.ts,
                    'ampp_id': self.ampp_id,
                    'ampp_type': self.ampp_type,
                    'device_ip': self.device_ip}

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _ampq_connect(self):
        try:
            self.__amqp_sender_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop, timeout=5)
            self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
            self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('integration', ExchangeType.TOPIC)
            self.__amqp_status = True
            await self.__logger.info({'module': self.name, 'AMQP Connection': self.__amqp_sender_cnx})

        except Exception as e:
            await self.__logger.error(e)
            self.__amqp_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict, key: str):
        body = json.dumps(data).encode()
        await self.__amqp_sender_ex.publish(Message(body, delivery_mode=DeliveryMode.PERSISTENT), routing_key='status')

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
                # try:
                res = await self.eventloop.run_in_executor(None, self._ping, device['terIp'])
                ping_object.value_ = res
                asyncio.ensure_future(self._amqp_send(ping_object.data, 'status'))
                await asyncio.sleep(0.1)
            await asyncio.sleep(snmp_polling)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._ampq_connect())
        self.eventloop.run_until_complete(self._dispatch())
