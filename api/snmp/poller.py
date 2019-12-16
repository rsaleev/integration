import asyncio
import aiosnmp
from aiosnmp.exceptions import SnmpErrorNoSuchName, SnmpTimeoutError
from datetime import datetime
from aio_pika import robust_connection, ExchangeType, Message, DeliveryMode
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import json
from configuration import snmp_retries, snmp_timeout, snmp_polling, is_cnx, amqp_host, amqp_port, amqp_user, amqp_password, sys_log
from .mibs import polling_mibs
import signal


class AsyncSNMPPoller:

    def __init__(self, modules_l, devices_l):
        self.__amqp_sender_cn: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
        self.name = 'SNMPPoller'
        self.__modules = modules_l
        self.__devices = devices_l

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
        if self.__amqp_status:
            return True
        else:
            return False

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _ampq_connect(self):
        try:
            self.__amqp_sender_cn = await robust_connection.connect(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            self.__amqp_sender_ch = await self.__amqp_sender_cn.channel()
            self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('snmp', ExchangeType.DIRECT)
            is_queue = await self.__amqp_sender_ch.declare_queue('is_snmp')
            await is_queue.bind(exchange=self.__amqp_sender_ex, routing_key='get')
            await self.__logger.info(f"Connected to:{self.__amqp_sender_cn}")
            self.__amqp_status = True
        except Exception as e:
            await self.__logger.error(e)
            self.__amqp_status = False
            raise
        finally:
            return self

    async def _amqp_send(self, data: dict):
        body = json.dumps(data)
        await self.__amqp_sender_ex.publish(Message(body.encode()), routing_key='get')  # DeliveryMode.PERSISTENT

    async def _dispatch(self):
        while True:
            try:
                for device in self.__devices:
                    with aiosnmp.Snmp(host=device['terIp'], port=161, community="public", timeout=snmp_timeout, retries=snmp_retries) as snmp:
                        for res in await snmp.get([mib.oid for mib in polling_mibs]):
                            if not res.oid is None and not res.value is None:
                                snmp_object = next((mib for mib in polling_mibs if mib.oid == res.oid), None)
                                if not snmp_object is None:
                                    await self.__logger.debug(snmp_object.__dict__)
                                    snmp_object.ts = datetime.now().timestamp()
                                    snmp_object.device_id = device['terId']
                                    snmp_object.device_type = device['terType']
                                    snmp_object.ampp_id = device['amppId']
                                    snmp_object.ampp_type = device['amppType']
                                    snmp_object.device_ip = device['terIp']
                                    if snmp_object.codename == "12VBoard" or "24VBoard":
                                        snmp_object.snmpvalue = res.value/10
                                    if snmp_object.codename == "24ABoard":
                                        snmp_object.snmpvalue = res.value/100
                                    else:
                                        snmp_object.snmpvalue = res.value
                                    asyncio.ensure_future(self._amqp_send(snmp_object.data))
            except Exception as e:
                await self.__logger.error(e)
                continue
            finally:
                await asyncio.sleep(snmp_polling)

    def _handler(self):
        asyncio.ensure_future(self._amqp_sender_cn.close())
        tasks = [t for t in asyncio.all_tasks() if t is not
                 asyncio.current_task()]
        [task.cancel() for task in tasks]
        self.eventloop.stop()

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._ampq_connect())
            self.eventloop.run_until_complete(self._dispatch())
        except KeyboardInterrupt:
            self.eventloop.close()
        # finally:
