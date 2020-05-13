from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
import json
import configuration as cfg


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
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__amqpconnector.bind('statuses', ['status.payment'])
        self.__cashiers = await self.__dbconnector_is.callproc('is_cashiers_get', rows=-1, values=[])
        inventories = await self.__dbconnector_wp.callproc('wp_inventory_get', rows=-1, values=[])
        tasks = []
        for inv in inventories:
            device = next(d for d in self.__cashiers if d['terId'] == inv['curTerId'])
            tasks.append(self.__dbconnector_is.allproc('is_inventory_ins', rows=0, values=[inv['curTerId'],
                                                                                           device['terAddress'], device['terDescription'], device['amppId'], inv['curChannelId'], inv['curChannelDescr']]))
        await asyncio.gather(*tasks)
        await self.__logger.info({'module': self.name, 'msg': 'Started...'})
        return self

    async def _process_payment(self, data: dict) -> None:
        payment_data = await self.__dbconnector_wp.callproc('wp_payment_get', rows=1, values=[data['terminal_id']])
        if not payment_data is None:
            msg = {'transacation_uid': data['tra_uid'],
                   'data': payment_data}
            # check working services and extend keys for outgoing message
            outgoing_keys = []
            services = await self.__dbconnector_is.callproc('is_services_get', rows=-1, values=[None, 1, None, None, None, 1, None, None])
            keys = [f"{s['serviceName']}.payment" for s in services]
            await self.__amqpconnector.send(msg,  persistent=True, keys=keys, priority=1)

    async def _process_money(self, data: dict) -> None:
        tasks = []
        money = await self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[data['device_id']])
        for m in money:
            tasks.append(self.__dbconnector_wp.callproc('wp_money_upd', rows=0, values=[m['curTerId'], m['curChannelId'], m['curValue'], m['curQuantity']]))
        await asyncio.gather(*tasks)

    async def _process_inventory(self, data: dict) -> None:
        tasks = []
        inventories = await self.__dbconnector_wp('wp_inventory_get', rows=-1, values=[data['device_id']])
        cashier = next(c for c in self.__cashiers if c['terId'] == data['device_id'])
        cashbox = next(inv['curTotal'] for inv in inventories if inv['channelId'] == 2)
        if cashier['storageLimit'] == cashbox:
            inv_warning = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'ALMOST_FULL')
            tasks.append(self.__amqpconnector.send(inv_warning.instance,  persistent=True, keys=['status.cashbox'], priority=10))
        else:
            inv_warning = self.InventoryWarning(data['device_id'], data['device_address'], data['device_ip'], data['device_type'], data['ampp_id'], data['ampp_type'], 'OK')
            tasks.append(self.__amqpconnector.send(inv_warning.instance,  persistent=True, keys=['status.cashbox'], priority=5))
        for inv in inventories:
            tasks.append(self.__dbconnector_is.callproc('is_invenotry_upd', rows=0, values=[data['device_id'], inv['curChannelId'], inv['curTotal']]))
        await asyncio.gather(*tasks)

    async def _process(self, redelivered, key, data) -> None:
        tasks = []
        if not redelivered:
            if data['value'] in ['FINISHED_WITH_SUCCESS', 'FINISHED_WITH_ISSUES']:
                tasks.append(self._process_payment(data))
                tasks.append(self._process_money(data))
                tasks.append(self._process_inventory(data))

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__amqpconnector.cbreceive(cb=self._process)
        else:
            await asyncio.sleep(0.5)
