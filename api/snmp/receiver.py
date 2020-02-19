
import asyncio
import aiosnmp
from datetime import datetime
import json
import configuration as cfg
from .mibs import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP


class AsyncSNMPReceiver:

    def __init__(self, devices):
        self.__amqpconnector = None
        self.__eventloop = None
        self.__logger = None
        self.__name = 'SNMPReceiver'
        self.__devices = devices

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
    def status(self):
        return self.__amqpconnector.connected

    @property
    def name(self):
        return self.__name

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log_debug)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection Status'})
        self.__amqpconnector = await AsyncAMQP(self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__logger.info({'module': self.name, 'AMQP Connection Status': self.__amqpconnector.connected})
        return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid = message.data.varbinds[1].value
            val = message.data.varbinds[2].value
            if host in [d['terIp'] for d in self.__devices] and oid in [m.oid for m in receiving_mibs]:
                device = next(dev for dev in self.__devices if dev['terIp'] == host)
                asyncio.ensure_future(device)
                snmp_object = next(mib for mib in receiving_mibs if mib.oid == oid)
                snmp_object.ts = datetime.now().timestamp()
                snmp_object.snmpvalue = val
                snmp_object.device_id = device['terId']
                snmp_object.device_address = device['terAddress']
                snmp_object.device_type = device['terType']
                snmp_object.ampp_id = device['amppId']
                snmp_object.ampp_type = device['amppType']
                snmp_object.device_ip = host
                if cfg.snmp_debug:
                    await self.__logger.debug(snmp_object.data)
                if snmp_object.codename == 'BarrierLoop1Status':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop1'], priority=10)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename == 'BarrierLoop2Status':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop2'], priority=10)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename == 'General':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.general'], priority=9)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename == 'UpperDoor' or snmp_object.codename == 'MiddleDoor':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.door'], priority=9)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename in ['IOBoard2.Temperatute', 'IOBoard2.Temperature', 'IOBoard3.Temperature']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.temperature'], priority=9)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename in ['Roboticket1', 'TicketPrinter1']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=9)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename == 'BarrierStatus':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.barrier'], priority=10)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename in ['FiscalPrinterIssues', 'FiscalPrinterBD']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fiscal'], priority=10)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename == 'SmartPayout':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=10)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename in ['CoinsHopper1', 'CoinsHopper2', 'CoinsHopper3']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=3)
                    await asyncio.sleep(0.2)
                elif snmp_object.codename in ['Coinbox']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=10)
                    await asyncio.sleep(0.2)
                else:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.trap'], priority=8)
                    await asyncio.sleep(0.2)
        except Exception as e:
            await self.__logger.error(e)
            await asyncio.sleep(0.2)

    async def _dispatch(self):
        trap_listener = aiosnmp.SnmpV2TrapServer(host=cfg.snmp_trap_host, port=cfg.snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
        self.eventloop.run_forever()
