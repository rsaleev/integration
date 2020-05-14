
import asyncio
import aiosnmp
from datetime import datetime
import json
import configuration as cfg
from .mibs import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from utils.asyncsql import AsyncDBPool
from uuid import uuid4
import signal
import os
import functools


class AsyncSNMPReceiver:

    def __init__(self):
        self.__amqpconnector = None
        self.__dbconnector_is = None
        self.__eventloop = None
        self.__logger = None
        self.name = 'SNMPReceiver'

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
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect())
        connections_tasks.append(AsyncDBPool(conn=cfg.is_cnx, min_size=5, max_size=10).connect())
        self.__amqpconnector, self.__dbconnector_is = await asyncio.gather(*connections_tasks)
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid = message.data.varbinds[1].value
            val = message.data.varbinds[2].value
            print(oid, '=', val)
            # check if valid device or is it unknown
            device = await self.__dbconnector_is.callproc('is_device_get', rows=1, values=[None, None, None, None, host])
            if not device is None:
                if oid in [m.oid for m in receiving_mibs]:
                    snmp_object = next(mib for mib in receiving_mibs if mib.oid == oid)
                    snmp_object.ts = datetime.now().timestamp()
                    snmp_object.snmpvalue = val
                    snmp_object.device_id = device['terId']
                    snmp_object.device_address = device['terAddress']
                    snmp_object.device_type = device['terType']
                    snmp_object.device_area = device['areaId']
                    snmp_object.ampp_id = device['amppId']
                    snmp_object.ampp_type = device['amppType']
                    snmp_object.device_ip = host
                    print(snmp_object.data)
                    # triggers that produce transaction event
                    if snmp_object.codename in 'BarrierLoop1Status':
                        # add uid
                        snmp_object.tra_uid = uuid4()
                        snmp_object.act_uid = uuid4()
                        if snmp_object.device_type == 1:
                            await self.__amqpconnector.send(data=snmp_object.data, persistent=True, keys=['status.loop1.entry'], priority=10)
                        elif snmp_object.device_type == 2:
                            await self.__amqpconnector.send(data=snmp_object.data, persistent=True, keys=['status..loop1.exit'], priority=10)
                    elif snmp_object.codename == 'PaymentAmount':
                        # add transaction uid
                        snmp_object.act_uid = uuid4()
                        snmp_object.tra_uid = uuid4()
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payment.amount'], priority=10)
                    elif snmp_object.codename == 'PaymentCardType':
                        snmp_object.act_uid = uuid4()
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payment.cardtype'], priority=10)
                    elif snmp_object.codename == 'PaymentStatus':
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.payment.finished'], priority=10)
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
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.coinhopper'], priority=3)
                    elif snmp_object.codename == 'Coinbox':
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, keys=['status.coinbox'], priority=3)
                    await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
        except Exception as e:
            print(e)
            await self.__logger.error(e)
            await asyncio.sleep(0.2)

    async def _dispatch(self):
        pid = os.getppid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid])
        trap_listener = aiosnmp.SnmpV2TrapServer(host=cfg.snmp_trap_host, port=cfg.snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

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
        # perform eventloop shutdown
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
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
            self.eventloop.run_forever()
        except asyncio.CancelledError:
            pass
