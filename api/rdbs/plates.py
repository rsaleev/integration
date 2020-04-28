from utils.asyncsql import AsyncDBPool
from datetime import date, timedelta
import configuration as cfg
import json
from datetime import datetime, timedelta
import asyncio
import signal
from contextlib import suppress
import os
from pprint import pprint


class PlatesDataProducer:
    def __init__(self):
        self.__gates = []
        self.__dbconnector_wp = None
        self.__dbconnector_is = None

    async def initialize(self):
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
        devices = await self.__dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
        self.__gates = [d for d in devices if d['terType'] in [1, 2]]
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
            d_out['camMode'] = 'trigger' if json.loads(device['terJSON'])['CameraMode'] == 1 else 'freerun'
            await self.__dbconnector_is.callproc('rep_plates_ins', rows=0,
                                                 values=[device['terAddress'], device['terDescription'], d_out['totalTransits'], d_out['more6symbols'], d_out['less6symbols'],
                                                         d_out['noSymbols'], d_out['accuracy'], d_out['camMode'], d_out['date']])

    async def _dispatch(self):
        while True:
            if 2 <= datetime.now().hour < 3:
                tasks = []
                for g in self.__gates:
                    tasks.append(self._fetch(g))
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(60)

    # graceful shutdown implementation
    async def _signal_handler(self, signal, loop):
        # catch signal
        await self.__logger.warning(f'{self.name} shutting down')
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__logger.shutdown()
        # stop loop
        self.__eventloop.stop()
        # cancel tasks
        pending = asyncio.Task.all_tasks()
        for task in pending:
            task.cancel()
            # Now we should await task to execute it's cancellation.
            # Cancelled task raises asyncio.CancelledError that we can suppress:
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

    # main function
    def run(self):
        # use policy for own event loop
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        # define signals
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(self._signal_handler(s, self.eventloop)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
            self.eventloop.run_forever()
        except:
            self.eventloop.close()
            os._exit(0)
