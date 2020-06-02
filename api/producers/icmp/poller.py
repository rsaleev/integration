import configuration.settings as cs
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
    def __init__(self):
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.__eventloop = None
        self.__eventsignal = False
        self.__logger = None
        self.name = 'PingPoller'

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
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        self.__amqpconnector, self.__dbconnector_is = await asyncio.gather(*connections_tasks)
        await self.__dbconnector_is.callproc('is_watchdog_ins', rows=0, values=[self.name, os.getpid(), 1, datetime.now()])
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
            await aioping.ping(device['terIp'], timeout=cs.IS_SNMP_TIMEOUT)
            network_status.value = 'ONLINE'
        except (TimeoutError, asyncio.TimeoutError):
            network_status.value = 'OFFLINE'
        finally:
            await self.__amqpconnector.send(network_status.data, persistent=True, keys=['status.online'], priority=7)

    async def _dispatch(self):
        while not self.eventsignal:
            try:
                devices = await self.__dbconnector_is.callproc('is_device_get', rows=-1, values=[None, None, None, None, None])
                tasks = []
                for d in devices:
                    tasks.append(self._process(d))
                await asyncio.gather(*tasks)
                await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
                await asyncio.sleep(cs.IS_RDBS_POLLING_INTERVAL)
            except asyncio.CancelledError:
                pass

        else:
            await asyncio.sleep(0.5)

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        await self.__amqpconnector.disconnect()
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(self._signal_cleanup(), return_exceptions=True)
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
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
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass
