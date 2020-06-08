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
import configuration.settings as cs
from integration.api.producers.snmp.poller import AsyncSNMPPoller
from integration.api.producers.snmp.receiver import AsyncSNMPReceiver
from integration.api.producers.icmp.poller import AsyncPingPoller
from integration.api.events.statuses import StatusListener
from integration.api.events.entry import EntryListener
from integration.api.events.exit import ExitListener
from integration.api.events.places import PlacesListener
from integration.api.events.payment import PaymentListener
from utils.asynclog import AsyncLogger
from integration.service import webservice
from integration.api.producers.rdbs.plates import PlatesDataMiner
import sys
from setproctitle import setproctitle
import sdnotify


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

    async def _initialize_info(self) -> None:
        # await self.__dbconnector_is.callproc('is_info_ins', rows=0, values=[cfg.object_id, cfg.ampp_parking_id, cfg.object_latitude, cfg.object_longitude, cfg.object_address])
        pass

    async def _initialize_server(self) -> None:
        ampp_id_mask = int(cs.AMPP_PARKING_ID) * 100
        service_version = await self.__soapconnector.client.service.GetVersion()
        await self.__soapconnector.execute('GetVersion')
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[0, 0, 0, 'server', ampp_id_mask+1, 1, 1, cs.WS_SERVER_IP, service_version['rVersion']])

    async def _initialize_device(self, device: dict, mapping: list) -> None:
        device_is = next(d for d in mapping if d['ter_id'] == device['terId'])
        ampp_id_mask = int(cs.AMPP_PARKING_ID) * 100
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[device['terId'], device['terAddress'], device['terType'], device_is['description'],
                                                                              ampp_id_mask+device_is['ampp_id'], device_is['ampp_type'], device['terIdArea'],
                                                                              device['terIPV4'], device['terVersion']])
        if device['terType'] in [1, 2]:
            if not device['terJSON'] is None and device['terJSON'] != '':
                config = json.loads(device['terJSON'])
                ocr_mode = 'unknown'
                if len(config.items()) > 0:
                    if config['CameraMode'] == 1:
                        ocr_mode = 'trigger'
                    elif config['CameraMode'] == 0:
                        ocr_mode = 'freerun'
                await self.__dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  ocr_mode, device['terCamPhoto1'],
                                                                                      device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                                      int(json.loads(device_is['barcode_reader_enabled']))
                                                                                      ])
            else:
                await self.__dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  'unknown', device['terCamPhoto1'],
                                                                                      device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                                      int(json.loads(device_is['barcode_reader_enabled']))
                                                                                      ])

        elif device['terType'] == 3:
            await self.__dbconnector_is.callproc('is_cashier_ins', rows=0, values=[device['terId'], device_is['cashbox_capacity'], device_is['cashbox_limit'], device_is['uniteller_id'],
                                                                                   device_is['uniteller_ip'], device_is['payonline_id'], device_is['payonline_ip'],
                                                                                   device_is['barcode_reader_ip'], int(json.loads(device_is['barcode_reader_enabled']))])

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
        n = sdnotify.SystemdNotifier()
        setproctitle('Wisepark Integration')
        try:
            self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
            await self.__logger.info('Starting...')
            connections_tasks = []
            self.__dbconnector_is = AsyncDBPool(cs.IS_SQL_CNX)
            self.__dbconnector_wp = AsyncDBPool(cs.WS_SQL_CNX)
            self.__soapconnector = AsyncSOAP(cs.WS_SOAP_USER, cs.WS_SOAP_PASSWORD, cs.WS_SERVER_ID, cs.WS_SOAP_TIMEOUT, cs.WS_SOAP_URL)
            connections_tasks.append(self.__dbconnector_is.connect())
            connections_tasks.append(self.__dbconnector_wp.connect())
            connections_tasks.append(self.__soapconnector.connect())
            self.__dbconnector_is, self.__dbconnector_wp, self.__soapconnector = await asyncio.gather(*connections_tasks)
            devices = await self.__dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
            f = open(cs.MAPPING, 'r')
            mapping = json.loads(f.read())
            f.close()
            tasks = []
            tasks.append(self._initialize_info())
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
            # payment listener
            payment_listener = PaymentListener()
            payment_listener_proc = Process(target=payment_listener.run, name=payment_listener.name)
            self.processes.append(payment_listener_proc)
            payment_listener_proc.start()
            # ping poller process
            icmp_poller = AsyncPingPoller()
            icmp_poller_proc = Process(target=icmp_poller.run, name=icmp_poller.name)
            self.processes.append(icmp_poller_proc)
            icmp_poller_proc.start()
            # SNMP poller process
            snmp_poller = AsyncSNMPPoller()
            snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
            self.processes.append(snmp_poller_proc)
            snmp_poller_proc.start()
            # SNMP receiver process
            snmp_receiver = AsyncSNMPReceiver()
            snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
            self.processes.append(snmp_receiver_proc)
            snmp_receiver_proc.start()
            # webservice
            webservice_proc = Process(target=webservice.run, name=webservice.name)
            self.processes.append(webservice_proc)
            webservice_proc.start()
            # Reports generators
            plates_reporting = PlatesDataMiner()
            plates_reporting_proc = Process(target=plates_reporting.run, name='plates_reporting')
            plates_reporting_proc.start()
            self.processes.append(plates_reporting_proc)
            # # log parent process status
            cleaning_tasks = []
            cleaning_tasks.append(self.__dbconnector_is.disconnect())
            cleaning_tasks.append(self.__dbconnector_wp.disconnect())
            cleaning_tasks.append(self.__logger.info('Started'))
            await asyncio.gather(*cleaning_tasks)
            n.notify("READY=1")
        except Exception as e:
            n.notify("READY=0")
            raise e

    async def _signal_handler(self, signal):
        shutdown_ready = False
        for p in self.processes:
            p.terminate()
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
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
            # self.eventloop.run_forever()
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    app = Application()
    app.run()
