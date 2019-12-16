import asyncio
import aiosnmp
from datetime import datetime
from aio_pika import connect, ExchangeType, Message
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import json
from config.configuration import snmp_retries, snmp_timeout, snmp_polling, integration_conn, amqp_host, amqp_port, amqp_user, amqp_password, sys_log
from snmp.objects import polling_mibs


class AsyncSNMPPoller:

    def __init__(self):
        self.__port = int
        self.__host = int
        self.__dbconnector = None
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
        self.__sql_status = bool
        self.__devices = list
        self.__name = 'TrapReceiver'

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
        if self.__amqp_status and self.__slq_status:
            return True
        else:
            return False

    @property
    def name(self):
        return self.__name

    @property
    def devices(self):
        return self.__devices

    @devices.setter
    def setter(self, value: list):
        self.__devices = value

    @devices.getter
    def getter(self):
        return self.__devices

    async def _logger_activate(self):
        self.__logger = AsyncLogger().getlogger(sys_log)
        return self

    async def _sql_connect(self):
        await self.__logger.info('Starting RDBS Pool Connection')
        try:
            self.__dbconnector = await AsyncDBPool(size=10, name='get_retranslator', conn=integration_conn, loop=self.eventloop)
            self.__sql_status = True
        except Exception as e:
            await self.__logger.error(e)
            self.__sql_status = False
        finally:
            await self.__logger.info(self.__dbconnector.connected)
            return self

    async def _device_mapping(self):
        await self.__logger.info('Fetching device mapping')
        self.__devices = await self.__dbconnector.callproc("wp_devices_get", None, [])
        return self.__devices

    async def _ampq_connect(self):
        try:
            self.__amqp_sender_cnx = await connect(user=amqp_user, password=amqp_password,
                                                   host=amqp_host, loop=self.eventloop)
            self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
            self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange(
                'gets', ExchangeType.FANOUT)
            modules = await self.__dbconnector.callproc('is_services_get', None, [None, 1, 1])
            ampp_module = next(module['enabled'] for module in modules if module['service'] == 'ampp')
            notifier_module = next(module['enabled'] for module in modules if module['service'] == 'notifier')
            if ampp_module:
                queue_ampp = await self.__amqp.sender_ch.declare_queue('ampp_get', exclusive=True)
                queue_ampp.bind(self.__amqp_sender_ex)
            if notifier_module:
                notifier_queue = await self.__amqp.sender_ch.declare_queue('notifier_get', exclusive=True)
                notifier_queue.bind(self.__amqp_sender_ex)
            self.__amqp_status = True
        except Exception as e:
            await self.__logger.error(e)
            self.__amqp_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict):
        body = json.dumps(data)
        await self.__amqp_sender_ex.publish(Message(body), routing_key='walk')

    async def _dispatch(self):
        while True:
            for device in self.__devices:
                with aiosnmp.Snmp(host=device['terIp'], port=161, community="public", timeout=snmp_timeout, retries=snmp_retries) as snmp:
                    try:
                        for res in await snmp.get([mib.oid for mib in polling_mibs]):
                            if not res.oid is None and not res.value is None:
                                get_object = next((mib for mib in polling_mibs if mib.oid == res.oid), None)
                                if not get_object is None:
                                    get_object.snmpvalue = res.value
                                    get_object.ts = datetime.now().timestamp()
                                    asyncio.ensure_future(self._amqp_send(get_object.get))
                                    # store in DB
                                    asyncio.ensure_future(self.__dbconnector.callproc('wp_status_ins', None, [device['terId'], device['terType'],
                                                                                                              device['amppId'], device['amppType'], get_object.codename, get_object.snmpvalue, device['terIp']]))
                    except aiosnmp.SnmpErrorNoSuchname:
                        continue
            await asyncio.sleep(snmp_polling)
            continue
