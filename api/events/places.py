from utils.asyncsql import AsyncDBPool, ProgrammingError, IntegrityError, OperationalError
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import configuration as cfg
import asyncio
from datetime import datetime
import json
import signal
import os
import functools
import contextlib


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
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(conn=cfg.is_cnx).connect())
        connections_tasks.append(AsyncDBPool(conn=cfg.wp_cnx).connect())
        connections_tasks.append(AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        # listen for loop2 status and lost ticket payment
        await self.__amqpconnector.bind('places_signals', ['status.loop2.*', 'command.physchal.*'], durable=False)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid])
        places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
        tasks = []
        for p in places:
            tasks.append(self.__dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'],  p['areTotalPark'], p['areFreePark'], p['areType']]))
            if p['areId'] == cfg.main_area and p['areFreePark'] == 0:
                warning = self.PlacesWarning('FULL')
                tasks.append(self.__amqpconnector.send(data=warning.instance, persistent=True, keys=['status.places'], priority=10))
            elif p['areId'] == cfg.main_area and p['areFreePark'] > 0:
                warning = self.PlacesWarning('VACANT')
                tasks.append(self.__amqpconnector.send(data=warning.instance, persistent=True, keys=['status.places'], priority=3))
        await asyncio.gather(*tasks)
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _process(self, redelivered, key, data):
        tasks = []
        if key in ['status.loop2.entry', 'status.loop2.exit']:
            places = await self.__dbconnector_wp.callproc('wp_places_get', rows=-1, values=[data['device_area']])
            for p in places:
                if p['areType'] == 1:
                    tasks.append(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[p['areTotalPark'] - p['areFreePark'], None, None, p['areId']]))
                    if p['areId'] == 1 and p['areFreePark'] == 0:
                        warning = self.PlacesWarning('FULL')
                        tasks.append(self.__amqpconnector.send(data=warning, persistent=True, keys=['status.places'], priority=10))
                    elif p['areId'] == 1 and ['areFreeparl'] > 0:
                        warning = self.PlacesWarning('VACANT')
                        tasks.append(self.__amqpconnector.send(data=warning, persistent=True, keys=['status.places'], priority=3))
                elif p['areType'] == 3:
                    tasks.append(self.__dbconnector_is.callproc('is_places_upd', rows=0, values=[None, None, p['areTotalPark'] - p['areFreePark'], p['areId']]))
            await asyncio.gather(*tasks)
        elif key == 'command.physchal.in':
            await self.__dbconnector_is.callproc('is_places_decrease_upd', rows=0, values=[2, data['area_id']])
        elif key == 'command.physchal.out':
            await self.__dbconnector_is.callproc('is_places_increase_upd', rows=0, values=[2, data['area_id']])

    # dispatcher

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass
            except (IntegrityError, OperationalError, ProgrammingError) as e:
                await self.__logger.error({'module': self.name, 'error': repr(e)})
            finally:
                await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0])

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

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
