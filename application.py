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
import signal
import os
from datetime import datetime
import nest_asyncio
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
        with open(cfg.device_mapping) as f:
            mapping = json.load(f)
        # get enabled devices except OCR cameras
        if dbconnector_wp.connected and self.dbconnector_is.connected:
            wp_devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
            ampp_id_mask = cfg.ampp_parking_id * 100
            asyncio.ensure_future(self.dbconnector_is.callproc('is_clear', rows=0, values=[]))
            # try:
            asyncio.ensure_future(self.dbconnector_is.callproc('is_devices_ins', rows=0,
                                                               values=[0, 0, 0, 'server', ampp_id_mask+next(dm['ampp_id'] for dm in mapping['devices'] if dm['ter_id'] == 0),
                                                                       next(dm['ampp_type'] for dm in mapping['devices'] if dm['ter_id'] == 0), 1, cfg.server_ip,
                                                                       None, None, None, None, None, None, None]))

            for device in wp_devices:
                if device['terType'] == 1:
                    # create a record in IS DB table wp_devices
                    asyncio.ensure_future(self.dbconnector_is.callproc('is_devices_ins', rows=0,
                                                                       values=[device['terId'], device['terAddress'], device['terType'], device['terDescription'],
                                                                               ampp_id_mask+next((dm.get('ampp_id') for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),
                                                                               next((dm.get('ampp_type') for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),
                                                                               device['terIdArea'], device['terIPV4'], device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                                               device['terCamPhoto2'], None, None, None,
                                                                               ]))

                elif device['terType'] == 2:
                    # create a record in IS DB table wp_devices
                    asyncio.ensure_future(self.dbconnector_is.callproc('is_devices_ins', rows=0,
                                                                       values=[device['terId'], device['terAddress'], device['terType'], device['terDescription'],
                                                                               ampp_id_mask+next((dm.get('ampp_id') for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),
                                                                               next((dm.get('ampp_type') for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),
                                                                               device['terIdArea'], device['terIPV4'], device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                                               device['terCamPhoto2'], None, None, None,
                                                                               ]))
                elif device['terType'] == 3:
                    # create a record in IS DB table wp_devices
                    asyncio.ensure_future(self.dbconnector_is.callproc('is_devices_ins', rows=0,
                                                                       values=[device['terId'], device['terAddress'], device['terType'], device['terDescription'],
                                                                               ampp_id_mask+next((dm['ampp_id'] for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),
                                                                               next((dm['ampp_type'] for dm in mapping['devices'] if device['terId'] == dm['ter_id']), 0),  device['terIdArea'],
                                                                               device['terIPV4'], None, None, None, None,
                                                                               next((dm.get('imager') for dm in mapping['devices'] if dm['ter_id'] == device['terId']), 0),
                                                                               next((dm.get('payonline') for dm in mapping['devices'] if dm['ter_id'] == device['terId']), 0),
                                                                               next((dm.get('uniteller') for dm in mapping['devices'] if dm['ter_id'] == device['terId']), 0)
                                                                               ]))
            self.devices = await self.dbconnector_is.callproc('is_devices_get', rows=-1, values=[])
            for d in self.devices:
                if d['terType'] == 0:
                    for codename in mapping['statuses']['server']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0,
                                                           values=[0, 0, d['amppId'], d['amppType'], codename, '', d['terIp'], datetime.now()])
                elif d['terType'] == 1:
                    for codename in mapping['statuses']['entry']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0,
                                                           values=[d['terId'], d['terType'], d['amppId'], d['amppType'], codename, '', d['terIp'], datetime.now()])
                elif d['terType'] == 2:
                    for codename in mapping['statuses']['exit']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0,
                                                           values=[d['terId'], d['terType'], d['amppId'], d['amppType'], codename, '', d['terIp'], datetime.now()])
                elif d['terType'] == 3:
                    for codename in mapping['statuses']['autocash']:
                        await self.dbconnector_is.callproc('is_status_ins', rows=0,
                                                           values=[d['terId'], d['terType'], d['amppId'], d['amppType'], codename, '', d['terIp'], datetime.now()])
            # create record in DB table
            await self.logger.info(f"Discovered devices:{[device for device in self.devices]}")
            # initialize places
            places = await dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
            print(places)
            # create record in DB tables
            for p in places:
                await self.dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'], p['areDescription'], p['areTotalPark'], p['areFreePark'],
                                                                                    cfg.physically_challenged_total])
            await self.logger.info(f"Discovered places:{[pl for pl in places]}")
            dbconnector_wp.pool.terminate()
            await dbconnector_wp.pool.wait_closed()
            return self
            # except Exception as e:
            #     await self.logger.error({'module': self.name, 'error': repr(e)})

    """
    initializes processes with type 'fork' (native for *NIX)
    """
    async def proc_init(self):
        # statuses listener process
        statuses_listener = StatusListener()
        statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        # self.processes.append(statuses_listener_proc)
        # ping poller process
        icmp_poller = AsyncPingPoller(self.devices)
        icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
        # self.processes.append(icmp_poller_proc)
        # SNMP poller process
        snmp_poller = AsyncSNMPPoller(self.devices)
        snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        # self.processes.append(snmp_poller_proc)
        # SNMP receiver process
        snmp_receiver = AsyncSNMPReceiver(self.devices)
        snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        self.processes.append(snmp_receiver_proc)
        # Webservice
        asgi_service_proc = Process(target=webservice.run, name=webservice.name)
        # self.processes.append(asgi_service_proc)
        return self

    async def start(self):
        for p in self.processes:
            asyncio.ensure_future(self.logger.info(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    async def check(self):
        while True:
            for p in self.processes:
                asyncio.ensure_future(self.dbconnector_is.callproc('is_processes_ins', rows=0, values=[p.name, p.is_alive(), p.pid]))
            await asyncio.sleep(10)

    def stop(self, loop):
        for p in self.processes:
            loop.run_until_complete(self.logger.warning(f'Stopping process:{p.name}'))
            p.terminate()
            loop.stop()


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    app = Application(loop)
    loop.run_until_complete(app.log_init())
    # loop.run_until_complete(app.db_init())
    loop.run_until_complete(app.proc_init())
    loop.run_until_complete(app.start())
    loop.run_forever()
    # loop.run_until_complete(app.check())
