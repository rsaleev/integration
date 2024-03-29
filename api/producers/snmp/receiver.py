
import asyncio
import functools
import json
import os
import signal
from datetime import datetime
from uuid import uuid4

import aiosnmp
import uvloop
from setproctitle import setproctitle

import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool

from integration.api.producers.snmp.mibs import receiving_mibs


class AsyncSNMPReceiver:

    def __init__(self):
        self.__amqpconnector_is = None
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
        setproctitle('integration-snmp-receiver')
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        self.__amqpconnector_is, self.__dbconnector_is = await asyncio.gather(*connections_tasks, return_exceptions=True)
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        await self.__logger.info({'module': self.name, 'msg': 'Started'})
        return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid = message.data.varbinds[1].value
            val = message.data.varbinds[2].value
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
                    snmp_object.act_uid = uuid4()
                    # triggers that produce transaction event
                    # partition events to active that must contain transaction uid and passive that don't contain transaction uid
                    if snmp_object.codename == 'BarrierLoop1Status':
                        if snmp_object.snmpvalue == 'OCCUPIED':
                            snmp_object.tra_uid = uuid4()
                            if snmp_object.device_type == 1:
                                await self.__amqpconnector_is.send(data=snmp_object.data, persistent=True, keys=['status.loop1.entry'], priority=10)
                            elif snmp_object.device_type == 2:
                                await self.__amqpconnector_is.send(data=snmp_object.data, persistent=True, keys=['status.loop1.exit'], priority=10)
                        elif snmp_object.snmpvalue == 'FREE':
                            if snmp_object.device_type == 1:
                                await self.__amqpconnector_is.send(data=snmp_object.data, persistent=True, keys=['status.loop1.entry'], priority=10)
                            elif snmp_object.device_type == 2:
                                await self.__amqpconnector_is.send(data=snmp_object.data, persistent=True, keys=['status.loop1.exit'], priority=10)
                    elif snmp_object.codename == 'PaymentType':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.type'], priority=10)
                    elif snmp_object.codename == 'PaymentAmount':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.amount'], priority=10)
                    elif snmp_object.codename == 'PaymentCardType':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.cardtype'], priority=10)
                    elif snmp_object.codename == 'PaymentStatus':
                        if snmp_object.snmpvalue == 'ZONE_PAYMENT':
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.proceeding'], priority=10)
                        elif snmp_object.snmpvalue == 'FINISHED_WITH_SUCCESS' or snmp_object.snmpvalue == 'FINISHED_WITH_ISSUES':
                            snmp_object.tra_uid = uuid4()
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.finished'], priority=10)
                        elif snmp_object.snmpvalue == 'PAYMENT_CANCELLED':
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payment.cancelled'], priority=10)
                    elif snmp_object.codename == 'BarrierLoop2Status':
                        # add uid
                        if snmp_object.device_type == 1:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.loop2.entry'], priority=10)
                        elif snmp_object.device_type == 2:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.loop2.exit'], priority=10)
                    elif snmp_object.codename == 'BarrierLoop1Reverse':
                        if snmp_object.device_type == 1:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.reverse.entry'], priority=10)
                        elif snmp_object.device_type == 2:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.reverse.exit'], priority=10)
                    elif snmp_object.codename == 'BarrierStatus':
                        if snmp_object.device_type == 1:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.barrier.entry'], priority=10)
                        elif snmp_object.device_type == 2:
                            await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.barrier.exit'], priority=10)
                    elif snmp_object.codename == 'General':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.general'], priority=9)
                    elif snmp_object.codename == 'UpperDoor' or snmp_object.codename == 'MiddleDoor':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.door'], priority=9)
                    elif snmp_object.codename in ['IOBoard2.Temperatute', 'IOBoard2.Temperature', 'IOBoard3.Temperature']:
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.temperature'], priority=9)
                    elif snmp_object.codename in ['Roboticket1', 'TicketPrinter1']:
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.tickets', 'event.tickets'], priority=9)
                    elif snmp_object.codename in ['FiscalPrinterIssues', 'FiscalPrinterBD']:
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.fiscal', 'event.fiscal'], priority=10)
                    elif snmp_object.codename == 'NotesReader':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.payout', 'event.payout'], priority=10)
                    elif snmp_object.codename in ['CoinsHopper1', 'CoinsHopper2', 'CoinsHopper3']:
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.coinhopper'], priority=3)
                    elif snmp_object.codename == 'Coinbox':
                        await self.__amqpconnector_is.send(snmp_object.data, persistent=True, keys=['status.coinbox', 'event.coinbox'], priority=3)
                    await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
        except Exception as e:
            await self.__logger.error({'module': self.name, 'exception': repr(e)})
        finally:
            await asyncio.sleep(0.2)

    async def _dispatch(self):
        # TODO: notify about server status 
        #pid = os.getpid()
        # await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        trap_listener = aiosnmp.SnmpV2TrapServer(host=cs.IS_SNMP_RECEIVER_HOST, port=cs.IS_SNMP_RECEIVER_PORT, communities=("public",), handler=self._handler)
        await trap_listener.run()

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__amqpconnector_is.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
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
        uvloop.install()
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
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
