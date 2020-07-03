from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import json
import configuration.settings as cs
import os
import sys
import functools
from uuid import uuid4
import re
from setproctitle import setproctitle
import uvloop


class PaymentListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__dbconnector_ws: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal = False
        self.name = 'PaymentListener'

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, v: bool):
        self.__eventloop = v

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v: bool):
        self.__eventsignal = v

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class InventoryWarning:
        def __init__(self, device_id, device_address, device_ip, device_type, ampp_id, ampp_type, value):
            self.__codename = 'Cashbox'
            self.__value: str = value
            self.__device_id: int = device_id
            self.__device_address: int = device_address
            self.__device_ip: str = device_ip
            self.__device_type: int = device_type
            self.__ampp_id: int = ampp_id
            self.__ampp_type: int = ampp_type
            self.__ts = datetime.now()

        @property
        def instance(self):
            return {'device_id': self.__device_id,
                    'device_address': self.__device_address,
                    'device_type': self.__device_type,
                    'codename': self.__codename,
                    'value': self.__value,
                    'ts': self.__ts,
                    'ampp_id': self.__ampp_id,
                    'ampp_type': self.__ampp_type,
                    'device_ip': self.__device_ip}

    # primary initialization of logging and connections
    async def _initialize(self) -> None:
        setproctitle('is-payments')
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
        connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
        connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_ws, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('payment_signals', ['status.payment.*'], durable=True)
        cashiers = await self.__dbconnector_is.callproc('is_cashier_get', rows=-1, values=[None])
        tasks = []
        for c in cashiers:
            tasks.append(self._initialize_inventory(c))
        await asyncio.gather(*tasks)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
        await self.__logger.info({'module': self.name, 'msg': 'Started...'})
        return self

    async def _initialize_inventory(self, device: dict) -> None:
        inventories = await self.__dbconnector_ws.callproc('wp_inventory_get', rows=-1, values=[device['terId']])
        if not inventories is None:
            tasks = []
            for inv in inventories:
                if inv['curChannelId'] == 1:
                    tasks.append(self.__dbconnector_is.callproc('is_inventory_ins', rows=0, values=[inv['curTerId'],
                                                                                                    inv['curChannelId'], inv['curChannelDescr'], inv['curTotal'], None]))
                elif inv['curChannelId'] == 2:
                    tasks.append(self.__dbconnector_is.callproc('is_inventory_ins', rows=0, values=[inv['curTerId'],
                                                                                                    inv['curChannelId'], inv['curChannelDescr'], inv['curTotal'], device['cashboxLimit']]))
            money = await self.__dbconnector_ws.callproc('wp_money_get', rows=-1, values=[device['terId']])
            for m in money:
                tasks.append(self.__dbconnector_is.callproc('is_money_ins', rows=0, values=[device['terId'], m['curChannelId'], m['curChannelDescr'], m['curQuantity'], m['curValue']]))
            await asyncio.gather(*tasks)
            check_tasks = []
            check_tasks.append(self.__dbconnector_ws.callproc('wp_inventory_get', rows=-1, values=[device['terId']]))
            check_tasks.append(self.__dbconnector_ws.callproc('wp_money_get', rows=-1, values=[device['terId']]))
            check_tasks.append(self.__dbconnector_is.callproc('is_inventory_get', rows=-1, values=[device['terId']]))
            wp_inv, wp_money, is_inv = await asyncio.gather(*check_tasks)
            wp_cashbox = next(wp_i for wp_i in wp_inv if wp_i['curChannelId'] == 2)
            is_cashbox = next(is_i for is_i in is_inv if is_i['storageType'] == 2)
            proc_tasks = []
            if wp_cashbox['curTotal'] == is_cashbox['storageLimit']:
                inv_status = self.InventoryWarning(device['terId'], device['terAddress'], device['terIp'], device['terType'], device['amppId'], device['amppType'], 'ALMOST_FULL')
                proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
            elif wp_cashbox['curTotal'] == is_cashbox['storageContent']:
                inv_status = self.InventoryWarning(device['terId'], device['terAddress'], device['terIp'], device['terType'], device['amppId'], device['amppType'], 'FULL')
                proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
            elif wp_cashbox['curTotal'] != is_cashbox['storageLimit'] and wp_cashbox['curTotal'] != is_cashbox['storageContent']:
                inv_status = self.InventoryWarning(device['terId'], device['terAddress'], device['terIp'], device['terType'], device['amppId'], device['amppType'], 'OK')
                proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
            await asyncio.gather(*proc_tasks)

    async def _process_payment(self, data: dict, payment_data: dict) -> None:
        await self.__dbconnector_is.callproc('is_payment_ins', rows=0, values=[data['tra_uid'],
                                                                               data['device_id'], data['act_uid'], data['value'], payment_data, datetime.now()])
        # slip receipt parser
        if payment_data['payType'] == 'P':
            receipt_data = await self.__dbconnector_ws.callproc('wp_receipt_get', rows=1, values=[payment_data['paymentUID']])
            card_name = ''
            card_name = ''
            rrn = ''
            if not receipt_data is None:
                receipt = receipt_data['recText']
                card_num_regex = r"Номер карты\S\s\d{1,6}\S{6}\d{4}"
                card_num_matches = re.search(card_num_regex, receipt, re.MULTILINE)
                card_num = card_num_matches.group(0)[13:]
                rrn_regex = r"RRN\S\s\d{1,12}"
                rrn_matches = re.search(rrn_regex, receipt, re.MULTILINE)
                rrn = rrn_matches.group(0)[7:]
                card_name_regex = r"Application\S\s\w{10}"
                card_name_matches = re.search(card_name_regex, receipt, re.MULTILINE)
                card_name = card_name_matches.group(0)[13:]
            await self.__dbconnector_is.callproc('is_payment_data_upd', rows=0, values=[card_name, card_num, rrn])
        await self.__amqpconnector.send(data=data, persistent=True, keys=['event.payment.done'], priority=10)

    async def _process_money(self, data: dict) -> None:
        tasks = []
        money = await self.__dbconnector_ws.callproc('wp_money_get', rows=-1, values=[data['device_id']])
        for m in money:
            tasks.append(self.__dbconnector_is.callproc('is_money_upd', rows=0, values=[m['curTerId'], m['curChannelId'], m['curValue'], m['curQuantity'], m['payCreation']]))
        await asyncio.gather(*tasks)

    async def _process_inventory(self, data: dict) -> None:
        check_tasks = []
        check_tasks.append(self.__dbconnector_ws.callproc('wp_inventory_get', rows=-1, values=[data['device_id']]))
        check_tasks.append(self.__dbconnector_ws.callproc('wp_money_get', rows=-1, values=[data['device_id']]))
        check_tasks.append(self.__dbconnector_is.callproc('is_inventory_get', rows=-1, values=[data['device_id']]))
        wp_inv, wp_money, is_inv = await asyncio.gather(*check_tasks)
        wp_cashbox = next(wp_i for wp_i in wp_inv if wp_i['curChannelId'] == 2)
        is_cashbox = next(is_i for is_i in is_inv if is_i['storageType'] == 2)
        proc_tasks = []
        for inv in wp_inv:
            proc_tasks.append(self.__dbconnector_is.callproc('is_inventory_upd', rows=0, values=[data['device_id'], inv['curChannelId'], inv['curTotal']]))
        for m in wp_money:
            proc_tasks.append(self.__dbconnector_is.callproc('is_money_upd', rows=0, values=[m['curTerId'],
                                                                                             m['curChannelId'], m['curChannelDescr'], m['curValue'], m['curQuantity'], m['payCreation']]))
        if wp_cashbox['curTotal'] == is_cashbox['storageLimit']:
            inv_status = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'ALMOST_FULL')
            proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
        elif wp_cashbox['curTotal'] == is_cashbox['storageContent']:
            inv_status = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'FULL')
            proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
        elif wp_cashbox['curTotal'] != is_cashbox['storageLimit'] and wp_cashbox['curTotal'] != is_cashbox['storageContent']:
            inv_status = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'OK')
            proc_tasks.append(self.__amqpconnector.send(inv_status.instance,  persistent=True, keys='status.cashbox', priority=10))
        await asyncio.gather(*proc_tasks)

    async def _process(self, redelivered, key, data) -> None:
        tasks = []
        if not redelivered:
            if data['codename'] == 'PaymentStatus':
                if data['value'] == 'FINISHED_WITH_SUCCESS' or data['value'] == 'FINISHED_WITH_ISSUES':
                    try:
                        payment_data = await self.__dbconnector_ws.callproc('wp_payment_get', rows=1, values=[data['device_id']])
                        if payment_data['payType'] == 'C':
                            tasks.append(self._process_inventory(data))
                            tasks.append(self._process_money(data))
                            tasks.append(self._process_payment(data, payment_data))
                        elif payment_data['payType'] == 'P':
                            tasks.append(self._process_payment(data, payment_data))
                        elif payment_data['payType'] == 'M':
                            tasks.append(self._process_payment(data, payment_data))
                        await asyncio.gather(*tasks)
                    except Exception as e:
                        await self.__logger.exception({'module': self.name})
                        await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])

    async def _dispatch(self) -> None:
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass
            except asyncio.CancelledError:
                pass
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, 0, datetime.now()])

    async def _signal_cleanup(self) -> None:
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_ws.disconnect())
        closing_tasks.append(self.__amqpconnector.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal) -> None:
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        asyncio.ensure_future(self._signal_cleanup())
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close process
        sys.exit(0)

    def run(self) -> None:
        # use own event loop
        uvloop.install()
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
