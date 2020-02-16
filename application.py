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
import service.settings as ws
import signal
import os
from datetime import datetime
import nest_asyncio
from aiomysql import IntegrityError
nest_asyncio.apply()


class Application:
    def __init__(self, loop):
        self.processes = []
        self.modules = []
        self.devices = []
        self.logger = None
        self.dbconnector_is = None
        self.eventloop = loop
        self.name = 'application'

    async def start(self):
        self.logger = await AsyncLogger().getlogger(cfg.log)
        await self.logger.debug('Logging initialiazed')
        await self.logger.info("Establishing RDBS Integration Pool Connection...")
        self.dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        await self.logger.info(f"RDBS Integration Connection: {self.dbconnector_is.connected}")
        devices = await self.dbconnector_is.callproc('is_devices_get', rows=-1, values=[None, None, None, None, None])
        statuses listener process
        statuses_listener = StatusListener()
        statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        self.processes.append(statuses_listener_proc)
        # places listener
        places_listener = PlacesListener()
        places_listener_proc = Process(target=places_listener.run, name=places_listener.name)
        self.processes.append(places_listener_proc)
        # ping poller process
        icmp_poller = AsyncPingPoller(devices)
        icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
        self.processes.append(icmp_poller_proc)
        # SNMP poller process
        snmp_poller = AsyncSNMPPoller(devices)
        snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        self.processes.append(snmp_poller_proc)
        # # SNMP receiver process
        snmp_receiver = AsyncSNMPReceiver(devices)
        snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        self.processes.append(snmp_receiver_proc)

        for p in self.processes:
            asyncio.ensure_future(self.logger.info(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    def stop(self, loop):
        for p in self.processes:
            loop.run_until_complete(self.logger.warning(f'Stopping process:{p.name}'))
            p.terminate()
            loop.stop()

    def check(self, loop):
        pass


if __name__ == "__main__":
    #loop = asyncio.get_event_loop()
    #app = Application(loop)
    webservice.run()
    # loop.run_until_complete(app.start())
    # loop.run_forever()
