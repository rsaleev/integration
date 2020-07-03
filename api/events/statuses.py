import asyncio
import contextlib
import functools
import json
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime

import uvloop
from setproctitle import setproctitle

import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool


class StatusListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__dbconnector_ws: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal = False
        self.name = 'StatusesListener'

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

    async def _initialize(self):
        setproctitle('integration-statuses')
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({"module": self.name, "info": "Starting..."})
        connections_tasks = []
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        self.__amqpconnector, self.__dbconnector_is = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('statuses', ['status.*', 'command.*.*'], durable=True)
        f = open(cs.MAPPING, 'r')
        mapping = json.loads(f.read())
        f.close()
        tasks = []
        for d_is in mapping['devices']:
            if d_is['ter_id'] == 0:
                for st in d_is['statuses']:
                    tasks.append(self.__dbconnector_is.callproc('is_status_ins', rows=0, values=[0, st]))
            if d_is['ter_id'] > 0:
                for st in d_is['statuses']:
                    tasks.append(self.__dbconnector_is.callproc('is_status_ins', rows=0, values=[d_is['ter_id'], st]))
        await asyncio.gather(*tasks)
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        await self.__logger.info({"module": self.name, "info": "Started"})
        return self

    # callback for post-processing AMQP message
    async def _process(self, redelivered, key, data):
        if not redelivered:
            await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value'], datetime.fromtimestamp(data['ts'])])

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, datetime.now()])
            await self.__amqpconnector.receive(self._process)

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, datetime.now()])
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__amqpconnector.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)
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
        sys.exit(0)

    def run(self):
        # use policy for own event loop

        uvloop.install()
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
