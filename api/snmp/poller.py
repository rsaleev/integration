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


class AsyncSNMPPoller:

    def __init__(self, devices_l):
        self.__amqp_cnx: object = None
        self.__amqp_ch: object = None
        self.__amqp_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
        self.name = 'SNMPPoller'
        self.__devices = devices_l
        self.__amqpconnector = None

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        self.__amqpconnector = await AsyncAMQP(self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        return self

    async def _dispatch(self):
        while True:
            for device in self.__devices:
                with aiosnmp.Snmp(host=device['terIp'], port=161, community="public", timeout=cfg.snmp_timeout, retries=cfg.snmp_retries) as snmp:
                    try:
                        for res in await snmp.get([mib.oid for mib in polling_mibs]):
                            snmp_object = next((mib for mib in polling_mibs if mib.oid == res.oid), None)
                            if not snmp_object is None:
                                snmp_object.ts = datetime.now().timestamp()
                                snmp_object.device_id = device['terId']
                                snmp_object.device_address = device['terAddress']
                                snmp_object.device_type = device['terType']
                                snmp_object.ampp_id = device['amppId']
                                snmp_object.ampp_type = device['amppType']
                                snmp_object.device_ip = device['terIp']
                                snmp_object.snmpvalue = res.value
                                # if snmp_object.codename == "BarrierLoop1Status":
                                #     snmp_object.uid = uuid4()
                                #     await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop1'], priority=6)
                                # elif snmp_object.codename == "BarrierLoop2Status":
                                #     snmp_object.uid = uuid4()
                                #    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop2'], priority=6)
                                # elif snmp_object.codename == 'BarrierStatus':
                                #     snmp_object.uid = uuid4()
                                #     await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.barrier'], priority=10)
                                if snmp_object.codename in ["AlmostOutOfPaper", "PaperDevice1", "PaperDevice2"]:
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.paper'], priority=7)
                                elif snmp_object.codename == 'General':
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.general'], priority=9)
                                elif snmp_object.codename == 'Heater':
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.heater'], priority=1)
                                elif snmp_object.codename == 'FanIn' or snmp_object.codename == 'FanOut':
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fan'], priority=1)
                                elif snmp_object.codename == 'UpperDoor' or snmp_object.codename == 'MiddleDoor':
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.door'], priority=8)
                                elif (snmp_object.codename == 'RoboTicket1' or snmp_object.codename == 'RoboTicket2' or
                                      snmp_object.codename == 'TicketPtinter1' or snmp_object.codename == 'TicketPrinter2'):
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.printer'], priority=8)
                                # elif snmp_object.codename == 'AlmostOutOfPaper':
                                #     await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=8)
                                elif snmp_object.codename == 'IOBoards':
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.ioboards'], priority=8)
                                # elif snmp_object.codename == 'PaperDevice1' or snmp_object.codename == 'PaperDevice2':
                                #     await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=5)
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
                        await asyncio.sleep(0.2)
                        pass

   # graceful shutdown implementation
    async def _handler(self, signal, loop):
        # catch signal
        await self.__logger.warning(f'{self.name} shutting down')
        await self.__logger.shutdown()
        await self.__amqpconnector.disconnect()
        # stop loop
        self.__eventloop.stop()
        # cancel tasks
        pending = asyncio.Task.all_tasks()
        for task in pending:
            task.cancel()
            # Now we should await task to execute it's cancellation.
            # Cancelled task raises asyncio.CancelledError that we can suppress:
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

    def run(self):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        # define signals
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(self._handler(s, self.eventloop)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
            self.eventloop.run_forever()
        except:
            self.eventloop.close()
            os._exit(0)
