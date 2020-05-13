from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import json
import functools
import os


class StatusListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
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
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Starting..,"})
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='direct').connect()
        await self.__amqpconnector.bind('statuses', ['status.*', 'command.*.*'], durable=True)
        self.__dbconnector_is = await AsyncDBPool(cfg.is_cnx).connect()
        await self.__logger.info({"module": self.name, "info": "Started"})
        return self

    # callback for post-processing AMQP message
    async def _process(self, redelivered, key, data):
        if not redelivered:
            await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[data['device_id'], data['codename'], data['value'], datetime.fromtimestamp(data['ts'])])
            await asyncio.sleep(0.5)

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__amqpconnector.cbreceive(self._process)
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.5)

    async def _signal_handler(self, signal):
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        list(map(lambda task: task.cancel(), tasks))
        await asyncio.gather(*tasks, return_exceptions=True)
        self.eventloop.stop()
        self.eventloop.close()
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
