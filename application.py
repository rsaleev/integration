import asyncio
import functools
import json
import os
import signal
import sys
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

import sdnotify
import toml

from setproctitle import setproctitle

import configuration.settings as cs
from integration.api.events.entry import EntryListener
from integration.api.events.exit import ExitListener
from integration.api.events.payment import PaymentListener
from integration.api.events.places import PlacesListener
from integration.api.events.statuses import StatusListener
from integration.api.producers.icmp.poller import AsyncPingPoller
from integration.api.producers.rdbs.plates import PlatesDataMiner
from integration.api.producers.snmp.poller import AsyncSNMPPoller
from integration.api.producers.snmp.receiver import AsyncSNMPReceiver
from integration.service import webservice
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool
from utils.logstash import LogStash


class Application:
    def __init__(self):
        self.processes = []
        self.__logger = None
        self.__dbconnector_is = None
        self.__dbconnector_ws = None
        self.__soapconnector_ws = None
        self.eventloop = None
        self.name = 'integration'

    async def _initialize_info(self, settings: dict) -> None:
        await self.__dbconnector_is.callproc('is_info_ins', rows=0,
                                             values=[
                                                 settings['wisepark']['site_id'],  # CAME site ID
                                                 settings['ampp']['id'],  # AMPP parking ID
                                                 settings['ampp']['coordinates'][0],  # latitude
                                                 settings['ampp']['coordinates'][1],  # longitude
                                                 settings['ampp']['address']  # street name,  street index
                                             ])

    async def _initialize_server(self, ampp_id_mask: int, mapping: list, settings: dict) -> None:
        # iterarate through devices
        device_is = next(d for d in mapping if d['ter_id'] == 0)
        # fetch data from SOAP method
        soap_version = await self.__soapconnector_ws.execute('GetVersion', header=True)
        # fetch data from RDBS procedure
        db_version = await self.__dbconnector_ws.callproc('wp_dbversion_get', rows=1, values=[])
        # insert collected data in Integration RDBS
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[
            0,  # ID
            0,  # address
            0,  # type
            'server',  # description
            ampp_id_mask+1,  # default AMPP ID $parking_id + 1
            device_is['ampp_type'],  # AMPP type
            1,  # area
            settings['wisepark']['server_ip'],
            f"{soap_version['rVersion']};{db_version['parDBVersion']}"  # version of services and version of DB
        ])

    async def _initialize_device(self, ampp_id_mask: int, device: dict, mapping: list) -> None:
        device_is = next(d for d in mapping if d['ter_id'] == device['terId'])
        # insert collected data in Integration RDBS
        await self.__dbconnector_is.callproc('is_device_ins', rows=0, values=[
            device['terId'],
            device['terAddress'],
            device['terType'],
            device_is['description'],
            ampp_id_mask+device_is['ampp_id'],
            device_is['ampp_type'],
            device['terIdArea'],
            device['terIPV4'],
            device['terVersion']
        ])
        # column in/out
        if device['terType'] in [1, 2]:
            # OCR camera mode is stored in column `terJSON` as JSON value {"CameraMode":1}
            ocr_mode = 'unknown'
            if not device['terJSON'] is None:
                stored_value = json.loads(device['terJSON'])
                if stored_value['CameraMode'] == 1:
                    ocr_mode = 'trigger'
                elif stored_value['cameraMode'] == 0:
                    ocr_mode = 'freerun'
            await self.__dbconnector_is.callproc('is_column_ins', rows=0, values=[
                device['terId'],
                device['terCamPlate'],
                ocr_mode,
                device['terCamPhoto1'],
                device['terCamPhoto2'],
                device_is['ticket_device'],  # ticket device description
                device_is['barcode_reader_ip'],  # IP address of custom barcode/UID reader
                int(device_is['barcode_reader_enabled'])
            ])
        # automatic cash
        elif device['terType'] == 3:
            await self.__dbconnector_is.callproc('is_cashier_ins', rows=0, values=[
                device['terId'],
                device_is['cashbox_capacity'],
                device_is['cashbox_limit'],
                device_is['uniteller_id'],
                device_is['uniteller_ip'],
                device_is['payonline_id'],
                device_is['payonline_ip'],
                device_is['barcode_reader_ip'],
                int(json.loads(device_is['barcode_reader_enabled']))
            ])

    async def _initialize(self):
        # # Python systemctl-daemon Python wrapper
        n = sdnotify.SystemdNotifier()
        # define custom process title
        setproctitle('is-main')
        config = toml.load(cs.CONFIG_FILE)
        self.__logger = AsyncLogger(f'{cs.LOG_PATH}/integration.log').getlogger()
        self.__dbconnector_is = AsyncDBPool(host=config['integration']['rdbs']['host'],
                                            port=config['integration']['rdbs']['port'],
                                            login=config['integration']['rdbs']['login'],
                                            password=config['integration']['rdbs']['password'],
                                            database=config['integration']['rdbs']['database'])
        self.__dbconnector_ws = AsyncDBPool(host=config['wisepark']['rdbs']['host'],
                                            port=config['wisepark']['rdbs']['port'],
                                            login=config['wisepark']['rdbs']['login'],
                                            password=config['wisepark']['rdbs']['password'],
                                            database=config['wisepark']['rdbs']['database'])
        self.__soapconnector_ws = AsyncSOAP(login=config['wisepark']['soap']['login'],
                                            password=config['wisepark']['soap']['password'],
                                            url=config['wisepark']['soap']['url'],
                                            timeout=config['wisepark']['soap']['timeout'])
        try:
            self.__logger = self.__logger.getlogger()
            await self.__logger.info('Starting...')
            # connect to RDBS and SOAP servers
            connections_tasks = []
            connections_tasks.append(self.__dbconnector_is.connect())
            connections_tasks.append(self.__dbconnector_ws.connect())
            connections_tasks.append(self.__soapconnector_ws.connect())
            await asyncio.gather(*connections_tasks)
            # load configuration
            # load devices settings
            devices_settings = toml.load(cs.DEVICES_FILE)
            # generate list of devices
            devices_mapping = [devices_settings['devices'].get(d) for d in devices_settings['devices']]
            # fetch devices configuration from wisepark DB
            devices_wisepark = await self.__dbconnector_ws.callproc('wp_devices_get', rows=-1, values=[])
            tasks = []
            ampp_mask = config['ampp']['id']*100
            tasks.append(self._initialize_info(config))
            tasks.append(self._initialize_server(ampp_mask, devices_mapping, config))
            for d in devices_wisepark:
                tasks.append(self._initialize_device(ampp_mask, d, devices_mapping))
            await asyncio.gather(*tasks)
            # initialize instances, convert to processes and start child processes
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
            # log stashing
            logs_stash = LogStash(cs.LOG_PATH)
            log_stash_proc = Process(target=logs_stash.run, name='log_stash')
            self.processes.append(log_stash_proc)
            # perform cleaning
            cleaning_tasks = []
            cleaning_tasks.append(self.__dbconnector_is.disconnect())
            cleaning_tasks.append(self.__dbconnector_ws.disconnect())
            cleaning_tasks.append(self.__soapconnector_ws.disconnect())
            cleaning_tasks.append(self.__logger.info('Started'))
            await asyncio.gather(*cleaning_tasks)
            if all([p.is_alive() for p in self.processes]):
                n.notify("READY=1")
        except Exception as e:
            self.__logger.exception({'module': self.name})
            n.notify("READY=0")
            sys.exit(repr(e))

    # signals handler
    async def _signal_handler(self, signal):
        for p in self.processes:
            p.terminate()
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        sys.exit(0)

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
