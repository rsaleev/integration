
import asyncio
import aiosnmp
from datetime import datetime
import json
import configuration as cfg
from .mibs import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from uuid import uuid4
import signal
from contextlib import suppress
import os


class AsyncSNMPReceiver:

    def __init__(self, devices):
        self.__amqpconnector = None
        self.__eventloop = None
        self.__logger = None
        self.name = 'SNMPReceiver'
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

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log_debug)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection Status'})
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__logger.info({'module': self.name, 'AMQP Connection Status': self.__amqpconnector.connected})
        return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid = message.data.varbinds[1].value
            val = message.data.varbinds[2].value
            if host in [d['terIp'] for d in self.__devices] and oid in [m.oid for m in receiving_mibs]:
                device = next(dev for dev in self.__devices if dev['terIp'] == host)
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
                # triggers that produce transaction event
                if snmp_object.codename in 'BarrierLoop1Status':
                    # add uid
                    snmp_object.tra_uid = uuid4()
                    snmp_object.act_uid = uuid4()
                    if snmp_object.device_type == 1:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop1.entry'], priority=10)
                    elif snmp_object.device_type == 2:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop1.exit'], priority=10)
                elif snmp_object.codename == 'PaymentAmount':
                    # add transaction uid
                    snmp_object.act_uid = uuid4()
                    snmp_object.tra_uid = uuid4()
                elif snmp_object.codename == 'PaymentStatus':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payment'], priority=10)
                # triggers that produce action uid but nor transaction uid
                elif snmp_object.codename == 'BarrierLoop2Status':
                    # add uid
                    snmp_object.act_uid = uuid4()
                    if snmp_object.device_type == 1:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop2.entry'], priority=10)
                    elif snmp_object.device_type == 2:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.loop2.exit'], priority=10)
                elif snmp_object.codename == 'BarrierStatus':
                    snmp_object.act_uid = uuid4()
                    if snmp_object.device_type == 1:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.barrier.entry'], priority=10)
                    elif snmp_object.device_type == 2:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.barrier.exit'], priority=10)
                elif snmp_object.codename == 'General':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.general'], priority=9)
                elif snmp_object.codename == 'UpperDoor' or snmp_object.codename == 'MiddleDoor':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.door'], priority=9)
                elif snmp_object.codename in ['IOBoard2.Temperatute', 'IOBoard2.Temperature', 'IOBoard3.Temperature']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.temperature'], priority=9)
                elif snmp_object.codename in ['Roboticket1', 'TicketPrinter1']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.tickets'], priority=9)
                elif snmp_object.codename in ['FiscalPrinterIssues', 'FiscalPrinterBD']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.fiscal'], priority=10)
                elif snmp_object.codename == 'SmartPayout':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=10)
                elif snmp_object.codename in ['CoinsHopper1', 'CoinsHopper2', 'CoinsHopper3']:
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=3)
                elif snmp_object.codename == 'Coinbox':
                    await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payout'], priority=10)

        except Exception as e:
            await self.__logger.error(e)
            await asyncio.sleep(0.2)

    async def _dispatch(self):
        trap_listener = aiosnmp.SnmpV2TrapServer(host=cfg.snmp_trap_host, port=cfg.snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

    # graceful shutdown implementation
    async def _signal_handler(self, signal, loop):
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
                s, lambda s=s: asyncio.create_task(self._signal_handler(s, self.eventloop)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
            self.eventloop.run_forever()
        except:
            self.eventloop.close()
            os._exit(0)
