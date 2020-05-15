import asyncio
import aiosnmp
from aiosnmp.exceptions import SnmpErrorNoSuchName, SnmpTimeoutError, SnmpErrorResourceUnavailable
from datetime import datetime
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncamqp import AsyncAMQP
import json
import configuration as cfg
from .mibs import polling_mibs
import signal
from uuid import uuid4
import os
import functools
import pprint


class AsyncSNMPPoller:

    def __init__(self):
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.__logger = None
        self.name = 'SNMPPoller'
        self.__amqpconnector = None
        self.__dbconnector_is = None

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, value):
        self.__eventsignal = value

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Starting..."})
        connections_tasks = []
        connections_tasks.append(AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect())
        connections_tasks.append(AsyncDBPool(conn=cfg.is_cnx, min_size=1, max_size=5).connect())
        self.__amqpconnector, self.__dbconnector_is = await asyncio.gather(*connections_tasks)
        await self.__logger.info({"module": self.name, "info": "Started"})
        return self

    async def _process(self, device, oid):
        if not device['terId'] == 0:
            with aiosnmp.Snmp(host=device['terIp'], port=161, community="public", timeout=cfg.snmp_timeout, retries=cfg.snmp_retries) as snmp:
                try:
                    for res in await snmp.get(oid):
                        snmp_object = next(mib for mib in polling_mibs if mib.oid == res.oid)
                        snmp_object.ts = datetime.now().timestamp()
                        snmp_object.device_id = device['terId']
                        snmp_object.device_address = device['terAddress']
                        snmp_object.device_type = device['terType']
                        snmp_object.device_area = device['areaId']
                        snmp_object.ampp_id = device['amppId']
                        snmp_object.ampp_type = device['amppType']
                        snmp_object.device_ip = device['terIp']
                        snmp_object.snmpvalue = res.value
                        if snmp_object.codename == "BarrierLoop1Status":
                            snmp_object.uid = uuid4()
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop1'], priority=6)
                        elif snmp_object.codename == "BarrierLoop2Status":
                            snmp_object.uid = uuid4()
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop2'], priority=6)
                        elif snmp_object.codename == 'BarrierStatus':
                            snmp_object.uid = uuid4()
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.barrier'], priority=8)
                        elif snmp_object.codename in ["AlmostOutOfPaper", "PaperDevice1", "PaperDevice2"]:
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.paper'], priority=7)
                        elif snmp_object.codename == 'General':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.general'], priority=8)
                        elif snmp_object.codename == 'Heater':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.heater'], priority=1)
                        elif snmp_object.codename == 'FanIn' or snmp_object.codename == 'FanOut':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fan'], priority=1)
                        elif snmp_object.codename == 'UpperDoor' or snmp_object.codename == 'MiddleDoor':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.door'], priority=8)
                        elif (snmp_object.codename == 'RoboTicket1' or snmp_object.codename == 'RoboTicket2' or
                                snmp_object.codename == 'TicketPtinter1' or snmp_object.codename == 'TicketPrinter2'):
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.printer'], priority=8)
                        elif snmp_object.codename == 'AlmostOutOfPaper':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=8)
                        elif snmp_object.codename == 'IOBoards':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.ioboards'], priority=8)
                        elif snmp_object.codename == 'PaperDevice1' or snmp_object.codename == 'PaperDevice2':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=5)
                        elif snmp_object.codename == 'IOBoard1.Temperature' or snmp_object.codename == 'IOBoard2.Temperature':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.temperature'], priority=2)
                        elif snmp_object.codename == 'VoIP':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.voip'], priority=7)
                        elif snmp_object.codename in ['TicketReader1', 'TicketReader2']:
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.reader'], priority=7)
                        elif snmp_object.codename == 'Coinbox':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.cashbox'], priority=9)
                        elif snmp_object.codename in ['CubeHopper', 'CoinsReader', 'CoinsHoper1', 'CoinsHopper2', 'CoinsHopper3',
                                                      'CoinBoxTriggered']:
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.coins'], priority=1)
                        elif snmp_object.codename == 'UPS':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.ups'], priority=5)
                        elif snmp_object.codename == 'IOCCtalk':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fiscal'], priority=8)
                        elif snmp_object.codename == 'FiscalPrinterStatus':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fiscal'], priority=8)
                        elif snmp_object.codename == 'FiscalPrinterBD':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fiscal'], priority=8)
                        elif snmp_object.codename in ['12VBoard', '24VBoard', '24ABoard']:
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.boards'], priority=3)
                        elif snmp_object.codename == 'SmartPayout':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=7)
                        elif snmp_object.codename == 'NotesReader':
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=2)
                        else:
                            await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.others'], priority=1)
                # handle SNMP exceptions
                except (SnmpErrorNoSuchName, SnmpErrorResourceUnavailable, ValueError, SnmpTimeoutError) as e:
                    await self.__logger.error({'module': self.name, 'error': repr(e)})
                    await asyncio.sleep(0.2)
                    pass

    async def _dispatch(self):
        while not self.eventsignal:
            devices = await self.__dbconnector_is.callproc('is_device_get', rows=-1, values=[None, None, None, None, None])
            tasks = []
            for d in devices:
                if d['terType'] != 0:
                    statuses = await self.__dbconnector_is.callproc('is_status_get', rows=-1, values=[d['terId'], None])
                    oids = [p.oid for p in polling_mibs if p.codename in [s['stCodename'] for s in statuses]]
                    for oid in oids:
                        tasks.append(self._process(d, oid))
            await asyncio.gather(*tasks)
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
            await asyncio.sleep(cfg.snmp_polling)
        else:
            await asyncio.sleep(0.5)

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        await self.__amqpconnector.disconnect()
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(self._signal_cleanup(), return_exceptions=True)
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close process
        os._exit(0)

    def run(self):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        # define signals
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass