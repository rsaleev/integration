
import asyncio
import aiosnmp
from datetime import datetime
import json
from configuration import snmp_trap_host, snmp_trap_port, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode
from .mibs import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import signal


"""
 Implements asynchronous SNMP Trap receiver

Generates:
    snmp_object -- object
    see 'receiving_mibs' objects

Purpose:
    parses SNMP Trap and send it to AMQP queues through different exchanges.



"""


class AsyncSNMPReceiver:

    def __init__(self, devices):
        self.__dbconnector_is = None
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
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
        if self.__amqp_status:
            return True
        else:
            return False

    @property
    def name(self):
        return self.__name

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _amqp_connect(self):
        try:
            asyncio.ensure_future(self.__logger.info('Establishing RabbitMQ connection'))
            while self.__amqp_sender_cnx is None:
                self.__amqp_sender_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop, timeout=5)
            else:
                self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
                self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('integration', ExchangeType.TOPIC)
            self.__amqp_status = True
            await self.__logger.info({'module': self.name, 'AMQP Connection': self.__amqp_sender_cnx})
        except Exception as e:
            asyncio.ensure_future(self._logger.error(e))
            self.__amqp_status = False
        finally:
            return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid, value = next(((trap.oid, trap.value) for trap in message.data.varbinds), (None, None))
            if not oid is None:
                ter_id, ter_type, ter_ip, ter_ampp_id, ter_ampp_type = next(((device['terId'], device['terType'], device['terIp'], device['amppId'],
                                                                              device['amppType']) for device in self.__devices if device['terIp'] == host), (None, None, None, None, None))
                snmp_object = next((mib for mib in receiving_mibs if mib.oid == oid), None)
                asyncio.ensure_future(self.__logger.debug({'TRAP': snmp_object.data}))
                if not snmp_object is None and not ter_id is None:
                    snmp_object.ts = datetime.now().timestamp()
                    snmp_object.device_id = ter_id
                    snmp_object.device_type = ter_type
                    snmp_object.ampp_id = ter_ampp_id
                    snmp_object.ampp_type = ter_ampp_type
                    snmp_object.device_ip = host
                    body = json.dumps(snmp_object.data).encode()
                    if snmp_object.codename == 'BarrierLoop1Status':
                        asyncio.ensure_future(self._amqp_sender_ex.publish(Message(body=body, delivery_mode=DeliveryMode.PERSISTENT), routing_key='loop1'))
                    elif snmp_object.codename == 'BarrierLoop2Status':
                        asyncio.ensure_future(self._amqp_sender_ex.publish(Message(body=body, delivery_mode=DeliveryMode.PERSISTENT), routing_key='loop2'))
                    else:
                        asyncio.ensure_future(self.__amqp_sender_ex_statuses.publish(Message(body=body, delivery_mode=DeliveryMode.PERSISTENT), routing_key='status'))
        except Exception as e:
            await self.__logger.error(e)

    async def _dispatch(self):
        await self.__logger.info({self.name: self.status})
        trap_listener = aiosnmp.SnmpV2TrapServer(host=snmp_trap_host, port=snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._amqp_connect())
        if self.status:
            asyncio.ensure_future(self._dispatch())
            self.eventloop.run_forever()
        else:
            self.eventloop.stop()
            self.eventloop.close()
        raise Exception(f"{self.name} not started. Check logs")
