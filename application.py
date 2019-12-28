import asyncio
from pathlib import Path
import json
from multiprocessing import Process
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from configuration import sys_log, wp_cnx, is_cnx, device_mapping, ampp_parking_id, sys_log, physically_challenged_total
from api.snmp.poller import AsyncSNMPPoller
from api.snmp.receiver import AsyncSNMPReceiver
from api.events.transit import TransitEventListener
from api.events.payment import PaymentsListener
from api.events.statuses import StatusListener
from api.events.places import PlacesListener
from api.icmp.poller import AsyncPingPoller
from utils.asynclog import AsyncLogger
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

    async def log_init(self):
        self.logger = await AsyncLogger().getlogger(sys_log)
        await self.logger.debug('Logging initialiazed')
        return self.logger

    async def db_init(self):
        await self.logger.info("Establishing RDBS Wisepark Pool Connection...")
        dbconnector_wp = await AsyncDBPool(conn=wp_cnx, loop=self.eventloop).connect()
        await self.logger.info(f"RDBS Integration Connection: {dbconnector_wp.connected}")
        await self.logger.info("Establishing RDBS Integration Pool Connection...")
        self.dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
        await self.logger.info(f"RDBS Integration Connection: {self.dbconnector_is.connected}")
        await self.logger.info(f"Reading device mapping file")
        with open(device_mapping) as f:
            devices_map = json.load(f)
        # get enabled devices except OCR cameras
        if dbconnector_wp.connected and self.dbconnector_is.connected:
            wp_devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
            ampp_id_mask = ampp_parking_id * 100
            for device in wp_devices:
                for device_map in devices_map:
                    if device['terId'] == device_map['ter_id']:
                        # initialize gate unit
                        if device['terType'] in [1, 2]:
                            # create a record in IS DB table wp_devices
                            await self.dbconnector_is.callproc('wp_devices_ins', rows=0,
                                                               values=[device['terAddress'], device['terType'],
                                                                       ampp_id_mask+device_map['ampp_id'], device_map['ampp_type'], device['terIPV4'],
                                                                       device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                                       device['terCamPhoto2'], None, None, None, device['terIdArea'], device['terDescription']
                                                                       ])

                            for mib in device_map['mibs']:
                                await self.dbconnector_is.callproc('wp_status_ins', rows=0,
                                                                   values=[device['terAddress'], device['terType'],  ampp_id_mask+device_map['ampp_id'], device_map['ampp_type'],
                                                                           mib, '', device['terIPV4'], datetime.now()])
                        elif device['terType'] == 3:
                            # create a record in IS DB table wp_devices
                            await self.dbconnector_is.callproc('wp_devices_ins', rows=0,
                                                               values=[device['terAddress'], device['terType'],
                                                                       ampp_id_mask+device_map['ampp_id'], device_map['ampp_type'], device['terIPV4'],
                                                                       None, None, None, None, device_map.get('imager', None),
                                                                       device_map.get('payonline', None), device_map.get('uniteller', None), device['terIdArea'], device['terDescription']
                                                                       ])

                            for mib in device_map['mibs']:
                                await self.dbconnector_is.callproc('wp_status_ins', rows=0,
                                                                   values=[device['terAddress'], device['terType'], ampp_id_mask+device_map['ampp_id'], device_map['ampp_type'],
                                                                           mib, '', device['terIPV4'], datetime.now()])
            # get modules that enabled
            self.modules = await self.dbconnector_is.callproc('is_modules_get', rows=-1, values=[None, 1, 1])
            # store devices
            self.devices = await self.dbconnector_is.callproc('wp_devices_get', rows=-1, values=[])
            # create record in DB table
            asyncio.ensure_future(self.logger.info(f"Discovered devices:{[device for device in self.devices]}"))
            # initialize places
            places = await dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
            # create record in DB tables
            for p in places:
                asyncio.ensure_future(self.dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areTotalPark'], p['areFreePark'], physically_challenged_total, p['areFloor'], p['areNumber']]))
            await self.logger.info(f"Discovered places:{[p for p in places]}")
            dbconnector_wp.pool.terminate()
            await dbconnector_wp.pool.wait_closed()
            return self

    """
    initializes processes with type 'fork' (native for *NIX)
    """
    async def proc_init(self):
        # Ping poller process
        icmp_poller = AsyncPingPoller(self.devices)
        icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
        # self.processes.append(icmp_poller_proc)
        # SNMP receiver process
        snmp_receiver = AsyncSNMPReceiver(self.modules, self.devices)
        snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        self.processes.append(snmp_receiver_proc)
        # SNMP poller process
        # snmp_poller = AsyncSNMPPoller(self.modules, self.devices)
        # snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        # #self.processes.append(snmp_poller_proc)
        # # statuses listener process
        # statuses_listener = StatusListener()
        # statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        # #self.processes.append(statuses_listener_proc)
        # # entry listener process
        # transit_listener = TransitEventListener(self.modules, self.devices)
        # transit_listener_proc = Process(target=transit_listener.run, name=transit_listener.name)
        # self.processes.append(transit_listener_proc)
        # # payment listener process
        # payment_listener = PaymentsListener(self.modules, self.devices)
        # payment_listener_proc = Process(target=payment_listener.run, name=payment_listener.name)
        # self.processes.append(payment_listener_proc)
        # # places listener process
        # places_listener = PlacesListener(self.modules)
        # places_listener_proc = Process(target=places_listener.run, name=places_listener.name)
        # self.processes.append(places_listener_proc)
        return self

    async def start(self):
        for p in self.processes:
            asyncio.ensure_future(self.logger.warning(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    async def check(self):
        while True:
            for p in self.processes:
                # asyncio.ensure_future(self.logger.debug({'process': p.pid, 'name': p.name, 'status': p.is_alive()}))
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
    loop.run_until_complete(app.db_init())
    loop.run_until_complete(app.proc_init())
    loop.run_until_complete(app.start())
    loop.run_until_complete(app.check())

    # loop.run_forever()
    # except (KeyboardInterrupt, SystemExit):
    #     for p in app.processes:
    #         app.stop(loop)
