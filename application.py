import asyncio
from pathlib import Path
import json
from multiprocessing import Process
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from configuration import sys_log, wp_cnx, is_cnx, device_mapping
from api.snmp.poller import AsyncSNMPPoller
from api.snmp.receiver import AsyncSNMPReceiver
from api.consumers.transit import TransitEventListener
from api.consumers.payment import PaymentsListener
from api.consumers.statuses import StatusListener
import signal
import os
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
        await self.logger.debug(devices_map)
        if dbconnector_wp.connected and self.dbconnector_is.connected:
            wp_devices = await dbconnector_wp.callproc('wp_devices_get', None, [])
            for device in wp_devices:
                for device_map in devices_map:
                    if device['terId'] == device_map['ter_id']:
                        await self.logger.debug(f'{device}, {device_map}')
                    if device['terId'] == device_map['ter_id']:
                        # initialize entry gate unit
                        if device['terType'] == 1:
                            # create a record in IS DB table wp_devices
                            await self.dbconnector_is.callproc('wp_devices_ins', None,
                                                               [device['terAddress'], device['terType'],
                                                                device_mapping['ampp_id'], device_mapping['ampp_type'], device['terIPV4'],
                                                                device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                                device['terCamPhoto2'], None, None, None, None
                                                                ])
                            # # create a record in IS DB table wp_statuses
                            # codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                            #              'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                            #              'UpperDoorOpen', 'VoIP', 'Roboticket1', 'Roboticket2', 'TicketReader1',
                            #              'TicketReader2', 'IOBoards', 'BarcodeReader1', 'BarcodeReader2',
                            #              'Loop1', 'Loop2', '12VBoard', '24VBoard', '24ABoard']
                            # for codename in codenames:
                            #     await self.dbconnector_is.callproc('wp_status_ins', None,
                            #                                        [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                            #                                         codename, '', device['terIPV4']])
                        # initialize exit gate unit
                        elif device['terType'] == 2:
                            # create a record in IS DB table wp_devices
                            await self.dbconnector_is.callproc('wp_devices_ins', None,
                                                               [device['terAddress'], device['terType'],
                                                                device_map['ampp_id'], device_map['ampp_type'], device['terIPV4'],
                                                                device['terCamPlate1'], device['terCamPlate2'], device['terCamPhoto1'],
                                                                device['terCamPhoto2'], None, None, None, None
                                                                ])
                            # # create a record in IS DB table wp_statuses
                            # codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                            #              'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                            #              'UpperDoor', 'VoIP', 'Roboticket1', 'Roboticket2', 'TicketReader1',
                            #              'TicketReader2', 'AlmostOutOfPaper', 'IOBoards', 'PaperDevice1', 'PaperDevice2',
                            #              'BarcodeReader1', 'BarcodeReader2', 'TicketPrinter1', 'TicketPrinter2',
                            #              'Loop1', 'Loop2', '12VBoard', '24VBoard', '24ABoard']
                            # for codename in codenames:
                            #     await self.dbconnector_is.callproc('wp_status_ins',
                            #                                        [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                            #                                         codename, '', device['terIPV4']])
                        # initialize autocash unit
                        elif device['terType'] == 3:
                            # create a record in IS DB table wp_devices

                            # terId, terType, amppId, amppType, terIp, camPlate1Ip, camPlate2Ip, camPhoto1Ip, camPhoto2Ip, barcodeReader1Ip, barcodeReader2Ip, payonlineIp, unitellerIp
                            await self.dbconnector_is.callproc('wp_devices_ins', None,
                                                               [device['terAddress'], device['terType'],
                                                                device_map['ampp_id'], device_map['ampp_type'], device['terIPV4'],
                                                                None, None, None, None, device_map.get('imager1', None), device_map.get('imager2', None),
                                                                device_map.get('payonline', None), device_map.get('uniteller', None)
                                                                ])
                            # create a record in IS DB table wp_statuses
                            # codenames = ['General', 'Heater', 'FanIn', 'FanOut', 'IOBoard1.Hummidity',
                            #              'IOBoard2.Humidity', 'IOBoard1.Temperature', 'IOBoard2.Temperature',
                            #              'UpperDoor', 'VoIP', 'Roboticket1', 'Roboticket2', 'Coinbox', 'CubeHopper', 'CCReader','AlmostOutOfPaper',
                            #              'IOBoards', 'CoinsReader', 'CoinsHopper1', 'CoinsHopper2', 'CoinsHopper3',
                            #              'BarcodeReader1', 'BarcodeReader2', 'TicketPrinter1', 'TicketPrinter2', 'NotesEscrow', 'NotesReader',
                            #              'IOCCtalk', 'UPS', 'FiscalPrinter', 'FiscalPrinterBD', 'MiddleDoor', 'CoinBoxTriggered', 'PaperDevice1',
                            #              '12VBoard', '24VBoard', '24ABoard']
                            # for codename in codenames:
                            #     await self.dbconnector_is.callproc('wp_status_ins', None,
                            #                                        [device['terAddress'], device['terType'], device_map['ampp_id'], device_map['ampp_type'],
                            #                                         codename, '', device['terIPV4']])
            # get list of enabled modules
            self.modules = await self.dbconnector_is.callproc('is_modules_get', None, [None, 1, 1])
            self.devices = await self.dbconnector_is.callproc('wp_devices_get', None, [])
            await self.logger.info(f"Discovered devices:{[device for device in self.devices]}")
            # self.logger.debug(is_devices)
            #self.modules = [record['module'] for record in records if record['enabled']]
            return self
            # dbconnector_wp.close()
            # await dbconnector_wp.wait_closed()
    """
    initializes processes with type 'fork' (native for *NIX)
    """
    async def proc_init(self):
        # SNMP receiver process
        #snmp_receiver = AsyncSNMPReceiver(self.modules, self.devices)
        #snmp_receiver_proc = Process(target=snmp_receiver.run, name=snmp_receiver.name)
        # self.processes.append(snmp_receiver_proc)
        # SNMP poller process
        snmp_poller = AsyncSNMPPoller(self.modules, self.devices)
        snmp_poller_proc = Process(target=snmp_poller.run, name=snmp_poller.name)
        self.processes.append(snmp_poller_proc)
        # entry listener process
        #transit_listener = TransitEventListener(self.modules, self.devices)
        #transit_listener_proc = Process(target=transit_listener.run, name=transit_listener.name)
        # self.processes.append(transit_listener_proc)
        # payment listener process
        #payment_listener = PaymentsListener(self.modules, self.devices)
        #payment_listener_proc = Process(target=payment_listener.run, name=payment_listener.name)
        # self.processes.append(payment_listener_proc)
        # statuses listener process
        statuses_listener = StatusListener()
        statuses_listener_proc = Process(target=statuses_listener.run, name=statuses_listener.name)
        self.processes.append(statuses_listener_proc)
        return self

    async def start(self):
        for p in self.processes:
            asyncio.ensure_future(self.logger.warning(f'Starting process:{p.name}'))
            p.start()
            await self.logger.info({'process': p.name, 'status': p.is_alive(), 'pid': p.pid})

    async def check(self, loop):
        while True:
            for p in self.processes:
                await self.dbconnector_is.callproc('is_processes_ins', None, [p.name, p.is_alive(), p.pid])
                await asyncio.sleep(10)

    # def stop(self, loop):
    #     for p in cls.processes:
    #         loop.run_until_complete(cls.logger.warning(f'Stopping process:{p.name}'))
    #         p.kill()


async def signal_handler(self, signal, loop):
    await self.logger.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
    for task in tasks:
            # skipping over shielded coro still does not help
        task.cancel()
    await self.dbconnector_is.pool.close()
    loop.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = Application(loop)
    # try:
    loop.run_until_complete(app.log_init())
    loop.run_until_complete(app.db_init())
    loop.run_until_complete(app.proc_init())
    loop.run_until_complete(app.start())
    loop.run_forever()
    # except KeyboardInterrupt:
    # pass
    # loop.close()
