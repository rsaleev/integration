import asyncio
from pathlib import Path
import json
from multiprocessing import Process
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import configuration as cfg
from api.snmp.poller import AsyncSNMPPoller
from api.snmp.receiver import AsyncSNMPReceiver
from api.events.statuses import StatusListener
from api.events.places import PlacesListener
from api.events.money import MoneyListener
from api.icmp.poller import AsyncPingPoller
from utils.asynclog import AsyncLogger
from service import webservice
import signal
from contextlib import suppress
import os
from datetime import datetime
from aiomysql import IntegrityError
from api.rdbs.plates import PlatesDataProducer
import psutil


class Application:
    def __init__(self, loop):
        self.processes = []
        self.modules = []
        self.devices = []
        self.logger = None
        self.dbconnector_is = None
        self.eventloop = loop
        self.alias = 'integration'

    async def _initialize(self):
        self.logger = await AsyncLogger().getlogger(cfg.log)
        await self.logger.debug('Logging initialiazed')
        await self.logger.info("Establishing RDBS Integration Pool Connection...")
        self.dbconnector_is = await AsyncDBPool(cfg.is_cnx).connect()
        await self.logger.info(f"RDBS Integration Connection: {self.dbconnector_is.connected}")
        devices = await self.dbconnector_is.callproc('is_devices_get', rows=-1, values=[None, None, None, None, None])
        # statuses listener process
        statuses_listener = StatusListener()
        statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        self.processes.append(statuses_listener_proc)
        # statuses_listener_proc.start()
        # # places listener
        places_listener = PlacesListener()
        places_listener_proc = Process(target=places_listener.run, name=places_listener.name)
        self.processes.append(places_listener_proc)
        # places_listener_proc.start()
        # ping poller process
        icmp_poller = AsyncPingPoller(devices)
        icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
        self.processes.append(icmp_poller_proc)
        # icmp_poller_proc.start()
        # SNMP poller process
        snmp_poller = AsyncSNMPPoller(devices)
        snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        self.processes.append(snmp_poller_proc)
        # snmp_poller_proc.start()
        # SNMP receiver process
        snmp_receiver = AsyncSNMPReceiver(devices)
        snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        # snmp_receiver_proc.start()
        self.processes.append(snmp_receiver_proc)
        webservice_proc = Process(target=webservice.run, name=webservice.name)
        webservice_proc.start()
        self.processes.append(webservice_proc)
        # Reports generators
        plates_reporting = PlatesDataProducer()
        plates_reporting_proc = Process(target=plates_reporting.run, name='plates_reporting')
        plates_reporting_proc.start()
        self.processes.append(plates_reporting_proc)
        # log parent process status
        await self.__dbconnector_is.callproc('is_services_ins', rows=0, values=[self.alias, os.getpid(), 1])

    async def check(self):
        for p in self.processes:
            asyncio.ensure_future(self.logger.info(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    async def _signal_handler(self, signal, loop):
        await self.__logger.warning(f'{self.alias} shutting down')
        await self.__dbconnector_is.disconnect()
        await self.__logger.shutdown()
        pending = asyncio.Task.all_tasks()
        for task in pending:
            pending = asyncio.Task.all_tasks()
            task.cancel()
            self.eventloop.stop()
            # Now we should await task to execute it's cancellation.
            # Cancelled task raises asyncio.CancelledError that we can suppress:
           # Now we should await task to execute it's cancellation.
            # Cancelled task raises asyncio.CancelledError that we can suppress:
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

    def stop(self):
        pid = os.getpid()
        parent = psutil.Process(pid)
        for children in parent.children():
            children.send_signal(signal.SIGTERM)

    def run(self):
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            self.eventloop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._signal_handler(s, self.eventloop)))
        try:
            self.eventloop.run_until_complete(self._initialize())
            tasks = [self.check(), self.external_check()]
            asyncio.gather(*tasks)
            self.eventloop.run_forever()
        except Exception as e:
            self.stop()
            self.eventloop.close()
            os._exit(0)
        except RuntimeError:
            pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = Application(loop)
    app.run()
