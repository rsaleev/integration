from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
import json
import configuration as cfg
import os
import functools


class PaymentListener:

    def __init__(self):
        self.__dbconnector_is: object = None
        self.__dbconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop = None
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
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(conn=cfg.is_cnx).connect())
        connections_tasks.append(AsyncDBPool(conn=cfg.wp_cnx).connect())
        connections_tasks.append(AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('payment_signals', ['status.payment.*'], durable=True)
        cashiers = await self.__dbconnector_is.callproc('is_cashier_get', rows=-1, values=[None])
        tasks = []
        for c in cashiers:
            tasks.append(self._initialize_inventory(c['terId'], c['cashboxLimit']))
        await asyncio.gather(*tasks)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid])
        await self.__logger.info({'module': self.name, 'msg': 'Started...'})
        return self

    async def _initialize_inventory(self, device_id, limit) -> None:
        inventories = await self.__dbconnector_wp.callproc('wp_inventory_get', rows=-1, values=[device_id])
        tasks = []
        for inv in inventories:
            if inv['curChannelId'] == 1:
                tasks.append(self.__dbconnector_is.callproc('is_inventory_ins', rows=0, values=[inv['curTerId'],
                                                                                                inv['curChannelId'], inv['curChannelDescr'], inv['curTotal'], None]))
            elif inv['curChannelId'] == 2:
                tasks.append(self.__dbconnector_is.callproc('is_inventory_ins', rows=0, values=[inv['curTerId'],
                                                                                                inv['curChannelId'], inv['curChannelDescr'], inv['curTotal'], limit]))
        money = await self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[device_id])
        for m in money:
            tasks.append(self.__dbconnector_is.callproc('is_money_ins', rows=0, values=[device_id, m['curChannelId'], m['curChannelDescr'], m['curQuantity'], m['curValue']]))
        await asyncio.gather(*tasks)

    async def _process_payment(self, data: dict) -> None:
        temp_data = await self.__dbconnector_is.callproc('is_payment_get', rows=1, values=[data['device_id']])
        if not temp_data is None:
            temp_data['ampp_id'] = data['ampp_id']
            temp_data['ampp_type'] = data['ampp_type']
            await self.__amqpconnector.send(temp_data,  persistent=True, keys=['payment.data'], priority=1)

    async def _process_money(self, data: dict) -> None:
        tasks = []
        money = await self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[data['device_id']])
        for m in money:
            tasks.append(self.__dbconnector_wp.callproc('wp_money_upd', rows=0, values=[m['curTerId'], m['curChannelId'], m['curValue'], m['curQuantity'], m['payCreation']]))
        await asyncio.gather(*tasks)

    async def _process_inventory(self, data: dict) -> None:
        check_tasks = []
        check_tasks.append(self.__dbconnector_wp.callproc('wp_inventory_get', rows=-1, values=[data['device_id']]))
        check_tasks.append(self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[data['device_id']]))
        check_tasks.append(self.__dbconnector_is.callproc('is_inventory_get', rows=-1, values=[data['device_id']]))
        wp_inv, wp_money, is_inv = await asyncio.gather(*check_tasks)
        wp_cashbox = next(wp_i for wp_i in wp_inv if wp_i['curChannelId'] == 2)
        is_cashbox = next(is_i for is_i in is_inv if is_i['storageType'] == 2)
        proc_tasks = []
        for inv in wp_inv:
            proc_tasks.append(self.__dbconnector_is.callproc('is_inventory_upd', rows=0, values=[data['device_id'], inv['curChannelId'], inv['curTotal']]))
        for m in wp_money:
            proc_tasks.append(self.__dbconnector_is.callproc('is_money_upd', rows=0, values=[m['curTerId'], m['curChannelId'], m['curValue'], m['curQuantity'], m['payCreation']]))
        await asyncio.gather(*proc_tasks)
        services = await self.__dbconnector_is.callproc('is_services_get', rows=-1, values=[None, 1, 1, None, None, None, None, None])
        if wp_cashbox['curTotal'] == is_cashbox['storageLimit']:
            inv_warning = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'ALMOST_FULL')
        else:
            inv_warning = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'OK')
            await self.__amqpconnector.send(inv_warning.instance,  persistent=True, keys='warning.autocash.inventory', priority=7)

    async def _process(self, redelivered, key, data) -> None:
        tasks = []
        if not redelivered:
            if data['codename'] == 'PaymentStatus':
                await self.__dbconnector_is.callproc('is_payment_ins', rows=0, values=[data['tra_uid'], data['device_address'], data['act_uid'], 'PAYMENT_TYPE_SELECTION', datetime.now()])
            elif data['codename'] == 'PaymentStatus':
                payment_data = await self.__dbconnector_wp.callproc('wp_payment_get', rows=1, values=[data['device_id']])
                if data['value'] == 'ZONE_PAYMENT':
                    await self.__dbconnector_is.callproc('is_payment_ins', rows=0, values=[data['tra_uid'], data['device_address'], data['act_uid'], 'PAYMENT_PROCESSING', datetime.now()])
                elif data['value'] == 'FINISHED_WITH_SUCCESS':
                    await self.__dbconnector_is.callproc('is_payment_data_upd', rows=0, values=[data['device_address'], json.dumps(payment_data, default=str)])
                    await self.__dbconnector_is.callproc('is_payment_status_upd', rows=0, values=[data['device_address'], 'PAYMENT_ENDED_WITH_SUCCESS', data['act_uid']])
                    temp_data = await self.__dbconnector_is.callproc('is_payment_get', rows=1, values=[data['device_address']])
                    temp_data['ampp_id'] = data['ampp_id']
                    temp_data['ampp_type'] = data['ampp_type']
                    tasks.append(self.__amqpconnector.send(data=temp_data, persistent=True, keys=['payment.ended'], priority=1))
                    tasks.append(self._process_payment(data))
                    tasks.append(self._process_inventory(data))
                elif data['value'] == 'FINISHED_WITH_ISSUES':
                    await self.__dbconnector_is.callproc('is_payment_data_upd', rows=0, values=[data['device_address'], json.dumps(payment_data, default=str)])
                    await self.__dbconnector_is.callproc('is_payment_status_upd', rows=0, values=[data['device_address'], 'PAYMENT_ENDED_WITH_ISSUES', data['act_uid']])
                    tasks.append(self.__amqpconnector.send(data=temp_data, persistent=True, keys=['payment.ended'], priority=1))
                    tasks.append(self._process_payment(data))
                    tasks.append(self._process_inventory(data))
                elif data['value'] == 'PAYMENT_CANCELLED':
                    temp_data = await self.__dbconnector_is('is_payment_get', rows=1, values=[data['device_address']])
                    temp_data['ampp_id'] = data['ampp_id']
                    temp_data['ampp_type'] = data['ampp_type']
                    await self.__dbconnector_is.callproc('is_payment_status_upd', rows=0, values=[data['device_address'], 'PAYMENT_CANCELLED', data['act_uid']])
                await asyncio.gather(*tasks)

    # dispatcher
    async def _dispatch(self) -> None:
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass
            except asyncio.CancelledError:
                pass
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0])

    async def _signal_cleanup(self) -> None:
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

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
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
