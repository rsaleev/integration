from utils.asyncsql import AsyncDBPool
from datetime import date, timedelta
import configuration.settings as cs
import json
from datetime import datetime, timedelta
import asyncio
import signal
import os
import functools


class PlatesDataMiner:
    def __init__(self):
        self.__gates = []
        self.__dbconnector_wp = None
        self.__dbconnector_is = None
        self.__eventsignal = None
        self.__eventloop = None
        self.__last_date = None

    @property
    def eventloop(self):
        return self.__eventloop

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
    def eventsignal(self, value):
        self.__eventsignal = value

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    async def _initialize(self):
        connection_tasks = []
        connection_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connection_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        self.__dbconnector_wp, self.__dbconnector_is = await asyncio.gather(*connection_tasks)
        columns = await self.__dbconnector_is.callproc('is_column_get', rows=-1, values=[None])
        date_today = date.today()
        first_day = date_today.replace(day=1)
        days_interval = date_today-first_day
        dates = [first_day + timedelta(days=x) for x in range(0, days_interval.days)]
        for c in columns:
            for d in dates:
                await asyncio.gather(self._fetch(c, d), return_exceptions=True)

    async def _fetch(self, device: dict, date: datetime):
        data_out = await self.__dbconnector_wp.callproc('rep_grz', rows=1, values=[device['terId'], date])
        if data_out is None:
            data_out = {'date': date, 'totalTransits': 0, 'more6symbols': 0, 'less6symbols': 0, 'less6symbols': 0, 'noSymbols': 0, 'accuracy': 0}
        else:
            data_out['noSymbols'] = data_out['totalTransits'] - data_out['more6symbols'] - data_out['less6symbols']
            if data_out['totalTransits'] > 0:
                data_out['accuracy'] = int(round(data_out['more6symbols']/data_out['totalTransits']*100, 2))
        await self.__dbconnector_is.callproc('rep_plates_ins', rows=0,
                                             values=[device['terAddress'], device['terType'], device['terDescription'], data_out['totalTransits'], data_out['more6symbols'], data_out['less6symbols'],
                                                     data_out['noSymbols'], data_out['accuracy'], device['camPlateMode'], data_out['date']])

    async def _dispatch(self):
        while not self.eventsignal:
            last_rep = await self.__dbconnector_is.callproc('rep_plates_last_get', rows=1, values=[])
            tasks = []
            columns = await self.__dbconnector_is.callproc('is_column_get', rows=-1, values=[None])
            if last_rep['repDate'] ==  date.today() - timedelta(days=1):
                date_today = date.today()ld
                days_interval = date_today - last_rep['repDate']
                dates = [last_rep['repDate'] + timedelta(days=x) for x in range(0, days_interval.days+1)]
                for c in columns:
                    for d in dates:
                        await asyncio.gather(self._fetch(c, d), return_exceptions=True)
                        await asyncio.sleep(0.5)

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__logger.shutdown()

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
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
        # try-except statement for signals
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass
