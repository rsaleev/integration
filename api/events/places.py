from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import configuration as cfg
import asyncio
import datetime
import json
import signal
import os
import functools


class PlacesListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.name = 'PlacesListener'

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
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v):
        self.__eventsignal = v

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class PlacesWarning:
        def __init__(self, value):
            self.__codename = 'Places'
            self.__value: str = value
            self.__device_id = 0
            self.__device_address = 0
            self.__device_ip = cfg.server_ip
            self.__device_type = 0
            self.__ampp_id = int(f'{cfg.ampp_parking_id}00')
            self.__ampp_type = 1
            self.__ts = datetime.now()

        @property
        def instance(self):
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
        await self.__logger.info({'module': self.name, 'info': 'Starting...'})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='direct').connect()
        # listen for loop2 status and lost ticket payment
        await self.__amqpconnector.bind('places_signals', ['status.loop2.*', 'command.physchal.*'], durable=False)
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self

    async def _process(self, redelivered, key, data):
        if key in ['status.loop2.entry', 'status.loop2.exit']:
            places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
            for p in places:
                tasks = []
                tasks.append(await self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areFreePark'], 1, p['areId']]))
                if p['areId'] == 1:
                    if p['areFreePark'] == 0:
                        msg = self.PlacesWarning(data['device_id'], data['device_address'], data['device_ip'], 'FULL')
                        send_tasks = []
                        send_tasks.append(self.__amqpconnector.send(msg,  persistent=True, keys=['status.server.places'], priority=10))
                        send_tasks.append(self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[0, 'Places', 'FULL', datetime.now()]))
                    else:
                        await self.__dbconnector_is.callproc('is_status_upd', rows=0, values=[0, 'Places', 'VACANT', datetime.now()])
        elif key == 'command.physchal.in':
            await self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areFreePark'], 2, p['areId']])

    # dispatcher

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__amqpconnector.cbreceive(self._process)
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.5)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        # stop while loop coroutine and send sleep signal to eventloop
        tasks = asyncio.all_tasks(self.eventloop)
        for t in tasks:
            t.cancel()
        # # perform cleaning tasks
        # cleaning_tasks = []
        # cleaning_tasks.append(asyncio.ensure_future(self.__logger.warning({'module': self.name, 'warning': 'Shutting down'})))
        # cleaning_tasks.append(asyncio.ensure_future(self.__dbconnector_is.disconnect()))
        # cleaning_tasks.append(asyncio.ensure_future(self.__dbconnector_wp.disconnect()))
        # cleaning_tasks.append(asyncio.ensure_future(self.__amqpconnector.disconnect()))
        # cleaning_tasks.append(asyncio.ensure_future(self.__logger.shutdown()))
        # pending = asyncio.all_tasks(self.eventloop)
        # # wait for cleaning tasks to be executed
        # await asyncio.gather(*pending, return_exceptions=True)
        # perform eventloop shutdown
        self.eventloop.stop()
        self.eventloop.close()
        # close process
        os._exit(0)

    def run(self):
        # use policy for own event loop
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future, (self._signal_handler(s))))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
