from utils.asyncsql import AsyncDBPool, ProgrammingError, IntegrityError, OperationalError
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import configuration.settings as cs
import asyncio
from datetime import datetime
import json
import signal
import os
import sys
import functools
import contextlib
from setproctitle import setproctitle
import uvloop


class PlacesListener:
    def __init__(self):
        self.__dbconnector_ws: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector_is: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal = False
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
        def __init__(self, codename, value):
            self.__codename = codename
            self.__value: str = value
            self.__device_id = 0
            self.__device_address = 0
            self.__device_ip = cs.WS_SERVER_IP
            self.__device_type = 0
            self.__ampp_id = int(f'{cs.AMPP_PARKING_ID}01')
            self.__ampp_type = 1
            self.__ts = datetime.now().timestamp()

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
        setproctitle('is-places')
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_ws, self.__amqpconnector_is = await asyncio.gather(*connections_tasks)
        # listen for loop2 status and lost ticket payment
        await self.__amqpconnector_is.bind('places_signals', ['status.loop2.entry', 'status.loop2.exit', 'event.challenged.in', 'event.challenged.out'], durable=True)
        places = await self.__dbconnector_ws.callproc('wp_places_get', rows=-1, values=[None])
        tasks = []
        for p in places:
            tasks.append(self.__dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'],  p['areTotalPark'], p['areFreePark'], p['areType']]))
            if p['areId'] == cs.WS_MAIN_AREA and p['areFreePark'] == 0:
                warning = self.PlacesWarning('PlacesOccasional', 'FULL')
                tasks.append(self.__amqpconnector_is.send(data=warning.instance, persistent=True, keys=['status.places.occasional'], priority=10))
            elif p['areId'] == cs.WS_MAIN_AREA and p['areFreePark'] > 0:
                warning = self.PlacesWarning('PlacesOccasional', 'FREE')
                tasks.append(self.__amqpconnector_is.send(data=warning.instance, persistent=True, keys=['status.places.occasional'], priority=3))
        tasks.append(self.__dbconnector_is.callproc('is_places_ins', rows=0, values=[]))
        await asyncio.gather(*tasks)
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _process(self, redelivered, key, data):
        try:
            pre_tasks = []
            post_tasks = []
            if key in ['status.loop2.entry', 'status.loop2.exit'] and data['value'] == 'OCCUPIED':
                pre_tasks.append(self.__dbconnector_ws.callproc('wp_places_get', rows=-1, values=[data['device_area']]))
                pre_tasks.append(self.__dbconnector_is.callproc('is_places_get'), rows=-1, values=[data['device_area']])
                ws_places, is_places = await asyncio.gather(*pre_tasks)
                for wp in ws_places:
                    post_tasks.append(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areTotalPark'] - p['areFreePark'], p['areType'], p['areId']]))
                    if wp['areId'] == cs.WS_MAIN_AREA and wp['areFreePark'] == 0:
                        warning = self.PlacesWarning('PlacesOccasional', 'FULL')
                        post_tasks.append(self.__amqpconnector_is.send(data=warning, persistent=True, keys=['status.places.occasional'], priority=10))
                    elif wp['areId'] == cs.WS_MAIN_AREA and ['areFreePark'] > 0:
                        warning = self.PlacesWarning('PlacesOccasional', 'VACANT')
                        post_tasks.append(self.__amqpconnector_is.send(data=warning, persistent=True, keys=['status.places.occasional'], priority=3))
            elif key == 'event.challenged.in':
                await self.__dbconnector_is.callproc('is_places_decrease_upd', rows=0, values=[2, data['area_id']])
                free_places = next(ip['freePlaces'] for ip in is_places if ip['clientType'] == 2)
                if free_places == 0:
                    warning = self.PlacesWarning('PlacesChallenged', 'FULL')
                    post_tasks.append(self.__amqpconnector_is.send(data=warning, persistent=True, keys=['status.places.challenged'], priority=10))
                else:
                    warning = self.PlacesWarning('Placeschallenged', 'VACANT')
                    post_tasks.append(self.__amqpconnector_is.send(data=warning, persistent=True, keys=['status.places.challenged'], priority=10))
            elif key == 'event.challenged.out':
                post_tasks.append(self.__dbconnector_is.callproc('is_places_increase_upd', rows=0, values=[2, data['area_id']]))
            await asyncio.gather(*post_tasks)
        except Exception as e:
            await self.__logger.exception({'module': self.name})
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 1, datetime.now()])

    # dispatcher

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])
            try:
                await self.__amqpconnector_is.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, 0, datetime.now()])
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_ws.disconnect())
        closing_tasks.append(self.__amqpconnector_is.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        asyncio.ensure_future(self._signal_cleanup())
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
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future, (self._signal_handler(s))))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
