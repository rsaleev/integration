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
import aioping
import signal
import functools


class AsyncPingPoller:
    def __init__(self, devices_l):
        self.__amqp_connector = None
        self.__eventloop = None
        self.__eventsignal = False
        self.__logger = None
        self.name = 'PingPoller'
        self.__devices = devices_l

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, v):
        self.__eventloop = v

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v):
        self.__eventsignal = v

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

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
        def value(self, v):
            self.__value = v

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

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        self.__amqp_connector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _process(self, device):
        network_status = self.NetworkStatus()
        network_status.device_id = device['terId']
        network_status.device_type = device['terType']
        network_status.device_ip = device['terIp']
        network_status.ampp_id = device['amppId']
        network_status.ampp_type = device['amppType']
        network_status.ts = datetime.now().timestamp()
        try:
            await aioping.ping(device['terIp'], timeout=cfg.snmp_timeout)
            network_status.value = 'ONLINE'
        except (TimeoutError, asyncio.TimeoutError):
            network_status.value = 'OFFLINE'
        finally:
            await self.__amqp_connector.send(network_status.data, persistent=True, keys=['status.online'], priority=1)

    async def _dispatch(self):
        while not self.eventsignal:
            try:
                for device in self.__devices:

                    await asyncio.sleep(0.2)
                    await asu
                await asyncio.sleep(cfg.snmp_polling)
            except asyncio.CancelledError:
                pass

    async def _signal_handler(self, signal):
        self.eventsignal = True
        # stop while loop coroutine
        self.eventsignal = True
        # stop while loop coroutine and send sleep signal to eventloop
        tasks = asyncio.all_tasks(self.eventloop)
        for task in tasks:
            task.cancel()
        # perform cleaning tasks
        cleaning_tasks = []
        cleaning_tasks.append(asyncio.ensure_future(self.__logger.warning({'module': self.name, 'warning': 'Shutting down'})))
        cleaning_tasks.append(asyncio.ensure_future(self.__dbconnector_is.disconnect()))
        cleaning_tasks.append(asyncio.ensure_future(self.__dbconnector_wp.disconnect()))
        cleaning_tasks.append(asyncio.ensure_future(self.__logger.shutdown()))
        pending = asyncio.all_tasks(self.eventloop)
        # wait for cleaning tasks to be executed
        await asyncio.gather(*pending)
        # perform eventloop shutdown
        self.eventloop.stop()
        self.eventloop.close()
        # close process
        os._exit(0)

    def run(self):
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
