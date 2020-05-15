import asyncio
from pathlib import Path
import json
import signal
import os
from datetime import datetime
import functools
from multiprocessing import Process
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncsoap import AsyncSOAP
import configuration as cfg
from api.producers.snmp.poller import AsyncSNMPPoller
from api.producers.snmp.receiver import AsyncSNMPReceiver
from api.producers.icmp.poller import AsyncPingPoller
from api.events.statuses import StatusListener
from api.events.entry import EntryListener
from api.events.exit import ExitListener
from api.events.places import PlacesListener
from api.events.payment import PaymentListener
from utils.asynclog import AsyncLogger
from service import webservice
from api.producers.rdbs.plates import PlatesDataMiner
import sys
import os


class Application:
    def __init__(self):
        self.processes = []
        self.modules = []
        self.devices = []
        self.__logger = None
        self.__dbconnector_is = None
        self.__dbconnector_wp = None
        self.__soapconnector = None
        self.eventloop = None
        self.alias = 'integration'

    async def _initialize_server(self) -> None:
        ampp_id_mask = cfg.ampp_parking_id * 100
        service_version = await self.__soapconnector.client.service.GetVersion()
        await self.__soapconnector.execute('GetVersion')
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[0, 0, 0, 'server', ampp_id_mask+1, 1, 1, cfg.server_ip, service_version['rVersion']])

    async def _initialize_device(self, device: dict, mapping: list) -> None:
        device_is = next(d for d in mapping if d['ter_id'] == device['terId'])
        ampp_id_mask = cfg.ampp_parking_id * 100
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[device['terId'], device['terAddress'], device['terType'], device_is['description'],
                                                                              ampp_id_mask+device_is['ampp_id'], device_is['ampp_type'], device['terIdArea'],
                                                                              device['terIPV4'], device['terVersion']])
        imager_enabled = 1 if device_is['barcode_reader_enabled'] else 0
        if device['terType'] in [1, 2]:
            if not device['terJSON'] is None:
                ocr_mode = json.loads(device['terJSON'])['CameraMode']
                ocr_mode_string = 'freerun' if ocr_mode == 1 else 'trigger'
                await self.__dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  ocr_mode, device['terCamPhoto1'],
                                                                                      device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                                      imager_enabled,
                                                                                      ])
            else:
                await self.__dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  'unknown', device['terCamPhoto1'],
                                                                                      device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                                      imager_enabled,
                                                                                      ])

        elif device['terType'] == 3:
            await self.__dbconnector_is.callproc('is_cashier_ins', rows=0, values=[device['terId'], device_is['cashbox_capacity'], device_is['cashbox_limit'], device_is['uniteller_id'],
                                                                                   device_is['uniteller_ip'], device_is['payonline_id'], device_is['payonline_ip'],
                                                                                   device_is['barcode_reader_ip'], 1 if device_is['barcode_reader_enabled'] else 0])

    async def _initialize_statuses(self, devices: list, mapping: dict) -> None:
        tasks = []
        for d_is in mapping:
            if d_is['ter_id'] == 0:
                for st in d_is['statuses']:
                    tasks.append(self.__dbconnector_is.callproc('is_status_ins', rows=0, values=[0, st]))
            if d_is['ter_id'] > 0:
                for st in d_is['statuses']:
                    tasks.append(self.__dbconnector_is.callproc('is_status_ins', rows=0, values=[d_is['ter_id'], st]))
        await asyncio.gather(*tasks)

    async def _initialize(self):
        try:
            self.__logger = await AsyncLogger().getlogger(cfg.log)
            await self.__logger.info('Starting...')
            connections_tasks = []
            connections_tasks.append(AsyncDBPool(cfg.wp_cnx).connect())
            connections_tasks.append(AsyncDBPool(cfg.is_cnx).connect())
            connections_tasks.append(AsyncSOAP(cfg.soap_user, cfg.soap_password, cfg.object_id, cfg.soap_timeout, cfg.soap_url).connect())
            self.__dbconnector_wp, self.__dbconnector_is, self.__soapconnector = await asyncio.gather(*connections_tasks)
            devices = await self.__dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
            f = open(cfg.MAPPING, 'r')
            mapping = json.loads(f.read())
            f.close()
            tasks = []
            tasks.append(self._initialize_server())
            for d in devices:
                tasks.append(self._initialize_device(d, mapping['devices']))
                tasks.append(self._initialize_statuses(d, mapping['devices']))
            await asyncio.gather(*tasks)
            # statuses listener process
            statuses_listener = StatusListener()
            statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
            self.processes.append(statuses_listener_proc)
            statuses_listener_proc.start()
            #  places listener
            places_listener = PlacesListener()
            places_listener_proc = Process(target=places_listener.run, name=places_listener.name)
            self.processes.append(places_listener_proc)
            places_listener_proc.start()
            # entry listener
            entry_listener = EntryListener()
            entry_listener_proc = Process(target=entry_listener.run, name=entry_listener.name)
            self.processes.append(entry_listener_proc)
            entry_listener_proc.start()
            # exit listener
            exit_listener = ExitListener()
            exit_listener_proc = Process(target=exit_listener.run, name=exit_listener.name)
            self.processes.append(entry_listener_proc)
            exit_listener_proc.start()
            # ping poller process
            icmp_poller = AsyncPingPoller()
            icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
            self.processes.append(icmp_poller_proc)
            icmp_poller_proc.start()
            # # SNMP poller process
            snmp_poller = AsyncSNMPPoller()
            snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
            self.processes.append(snmp_poller_proc)
            snmp_poller_proc.start()
            # # # SNMP receiver process
            snmp_receiver = AsyncSNMPReceiver()
            snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
            snmp_receiver_proc.start()
            self.processes.append(snmp_receiver_proc)
            # webservice
            webservice_proc = Process(target=webservice.run, name=webservice.name)
            webservice_proc.start()
            self.processes.append(webservice_proc)
            # #Reports generators
            plates_reporting = PlatesDataMiner()
            plates_reporting_proc = Process(target=plates_reporting.run, name='plates_reporting')
            plates_reporting_proc.start()
            self.processes.append(plates_reporting_proc)
            # # log parent process status
            await self.__dbconnector_is.callproc('is_services_ins', rows=0, values=[self.alias, os.getpid(), 1])
            cleaning_tasks = []
            cleaning_tasks.append(self.__dbconnector_is.disconnect())
            cleaning_tasks.append(self.__dbconnector_wp.disconnect())
            cleaning_tasks.append(self.__logger.info('Started'))
            await asyncio.gather(*cleaning_tasks)
        except asyncio.CancelledError:
            pass

    async def _signal_handler(self, signal):
        for p in self.processes:
            p.terminate()
        self.eventloop.close()
        os._exit(0)

    def run(self):
       # use own event loop
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # # try-except statement for signals
        try:
            self.eventloop.run_until_complete(self._initialize())
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    app = Application()
    app.run()
