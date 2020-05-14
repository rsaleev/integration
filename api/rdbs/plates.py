from utils.asyncsql import AsyncDBPool
from datetime import date, timedelta
import configuration as cfg
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
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
        return self

    async def _fetch(self, device: dict):
        date_today = date.today()
        first_day = date_today.replace(day=1)
        days_interval = date_today - first_day
        dates = [first_day + timedelta(days=x) for x in range(0, days_interval.days)]
        data_out = await self.__dbconnector_wp.callproc('rep_grz', rows=-1, values=[device['terId'], first_day, date_today])
        data_dates = [do['date'] for do in data_out]
        absent_dates = set(dates).difference(data_dates)
        for ad in absent_dates:
            data_out.append({'date': ad, 'totalTransits': 0, 'more6symbols': 0, 'less6symbols': 0, 'less6symbols': 0, 'noSymbols': 0, 'accuracy': 0})
        for d_out in data_out:
            d_out['noSymbols'] = d_out['totalTransits'] - d_out['more6symbols'] - d_out['less6symbols']
            if d_out['totalTransits'] > 0:
                d_out['accuracy'] = int(round(d_out['more6symbols']/d_out['totalTransits']*100, 0))
            d_out['camMode'] = device['camPlateMode']
            await self.__dbconnector_is.callproc('rep_plates_ins', rows=0,
                                                 values=[device['terAddress'], device['terDescription'], d_out['totalTransits'], d_out['more6symbols'], d_out['less6symbols'],
                                                         d_out['noSymbols'], d_out['accuracy'], d_out['camMode'], first_day, datetime.now()])

    async def _dispatch(self):
        while not self.eventsignal:
            if 20 <= datetime.now().hour < 21:
                tasks = []
                columns = await self.__dbconnector_is.callproc('is_column_get', rows=-1, values=[None])
                for c in columns:
                    tasks.append(self._fetch(c))
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(60)
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
