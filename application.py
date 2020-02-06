#!/usr/bin/python3

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

    async def log_init(self):
        self.logger = await AsyncLogger().getlogger(cfg.log)
        await self.logger.debug('Logging initialiazed')
        return self.logger

    async def db_init(self):
        await self.logger.info("Establishing RDBS Wisepark Pool Connection...")
        dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=self.eventloop).connect()
        await self.logger.info(f"RDBS Integration Connection: {dbconnector_wp.connected}")
        await self.logger.info("Establishing RDBS Integration Pool Connection...")
        self.dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=self.eventloop).connect()
        await self.logger.info(f"RDBS Integration Connection: {self.dbconnector_is.connected}")
        await self.logger.info(f"Reading device mapping file")
        try:
            with open(cfg.device_mapping) as f:
                mapping = json.load(f)
            devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
            ampp_id_mask = cfg.ampp_parking_id * 100
            await self.dbconnector_is.callproc('is_clear', rows=0, values=[])
            for dm in mapping['devices']:
                if dm['description'] == 'server':
                    await self.dbconnector_is.callproc('is_devices_ins', rows=0, values=[0, 0, 0, dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], 0,
                                                                                         cfg.server_ip, None, None, None, None,
                                                                                         dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])
                for d in devices:
                    if d['terAddress'] == dm['ter_addr']:
                        await self.dbconnector_is.callproc('is_devices_ins', rows=0, values=[d['terId'], d['terAddress'], d['terType'], dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], d['terIdArea'],
                                                                                             d['terIPV4'], d['terCamPlate1'], d['terCamPlate2'], d['terCamPhoto1'], d['terCamPhoto2'],
                                                                                             dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])

            self.devices = await self.dbconnector_is.callproc('is_devices_get', rows=-1, values=[])
            for di in self.devices:
                if di['terType'] == 0:
                    for ds in mapping['statuses']['server']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terType'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
                elif di['terType'] == 1:
                    for ds in mapping['statuses']['entry']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terType'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
                elif d['terType'] == 2:
                    for ds in mapping['statuses']['exit']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terType'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
                elif d['terType'] == 3:
                    for ds in mapping['statuses']['autocash']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terType'], di['terIp'], di['amppId'], di['amppType'], ds, ''])

            places = await dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
            with open(cfg.places_mapping) as f:
                mapped_places = json.load(f)
            for cp, p in zip(mapped_places['challenged'], places):
                if p['areId'] == cp['area']:
                    await self.dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'], p['areDescription'], p['areTotalPark'], p['areFreePark'], cp['total']])
        except Exception as e:
            await self.logger.error(e)
            pass

    async def proc_init(self):
        # statuses listener process
        statuses_listener = StatusListener()
        statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        self.processes.append(statuses_listener_proc)
        # ping poller process
        icmp_poller = AsyncPingPoller(self.devices)
        icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
        self.processes.append(icmp_poller_proc)
        # SNMP poller process
        snmp_poller = AsyncSNMPPoller(self.devices)
        snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        self.processes.append(snmp_poller_proc)
        # SNMP receiver process
        snmp_receiver = AsyncSNMPReceiver(self.devices)
        snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        self.processes.append(snmp_receiver_proc)
        # places listener
        places_listener = PlacesListener()
        places_listener_proc = Process(target=places_listener.run, name=places_listener.name)
        self.processes.append(places_listener_proc)
        webservice_proc = Process(target=webservice.run, name='webservice')
        self.processes.append(webservice_proc)

    async def start(self):
        for p in self.processes:
            asyncio.ensure_future(self.logger.info(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    def stop(self, loop):
        for p in self.processes:
            loop.run_until_complete(self.logger.warning(f'Stopping process:{p.name}'))
            p.terminate()
            loop.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = Application(loop)
    loop.run_until_complete(app.log_init())
    loop.run_until_complete(app.db_init())
    loop.run_until_complete(app.proc_init())
    loop.run_until_complete(app.start())
    loop.run_forever()
