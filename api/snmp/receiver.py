
import asyncio
import aiosnmp
from datetime import datetime
import json
from configuration import snmp_trap_host, snmp_trap_port, is_cnx, amqp_host, amqp_port, amqp_user, amqp_password, sys_log
from aio_pika import robust_connection, ExchangeType, Message, DeliveryMode
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

    def __init__(self, modules, devices):
        self.__dbconnector_is = None
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
        self.__name = 'SNMPReceiver'
        self.__modules = modules
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
            await self.__logger.info('Establishing RabbitMQ connection')
            self.__amqp_sender_cnx = await robust_connection.connect(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
            self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('snmp', ExchangeType.DIRECT)
            # queue for device trap except loop
            is_queue = await self.__amqp_sender_ch.declare_queue('is_snmp', exclusive=True, durable=True)
            await is_queue.bind(exchange=self.__amqp_sender_ex, routing_key='trap')
            # queue for loops traps for integration service
            is_transit_queue = await self.__amqp_sender_ch.declare_queue('is_loop_trap', exclusive=True, durable=True)
            await is_transit_queue.bind(exchange=self.__amqp_sender_ex, routing_key='loop')
            # for integration service triggers that will start coroutine of DB query for new transition
            is_transit_queue = await self.__amqp_sender_ch.declare_queue('is_places_trap', exclusive=True, durable=True)
            await is_transit_queue.bind(exchange=self.__amqp_sender_ex, routing_key='loop')
            is_lostticket_queue = await self.__amqp_sender_ch.declare_queue('is_lostticket_trap', exclusive=True, durable=True)
            await is_lostticket_queue.bind(exchange=self.__amqp_sender_ex, routing_key='lostticket')
            # queue for payments
            is_payment_queue = await self.__amqp_sender_ch.declare_queue('is_payment_trap', exclusive=True, durable=True)
            await is_payment_queue.bind(exchange=self.__amqp_sender_ex, routing_key='payment')
            # check for active modules. If module is active bind another queues
            if 'falcon' in self.__modules:
                falcon_queue = await self.__amqp_sender_ch.declare_queue('falcon_trap', exclusive=True, durable=True)
                await falcon_queue.bind(exchange=self.__amqp_sender_ex, routing_key='loop')
            # for notifier module we need everything except loop traps
            if 'notifier' in self.__modules:
                notifier_queue = await self.__amqp_sender_ch.declare_queue('notifier_trap', exclusive=True, durable=True)
                await notifier_queue.bind(self.__amqp_sender_ex, routing_key='trap')
            self.__amqp_status = True
            await self.__logger.info(f"RabbitMQ Connection:{self.__amqp_sender_cnx}")
        except Exception as e:
            await self._logger.error(e)
            self.__amqp_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict, key: str):
        body = json.dumps(data).encode()
        await self._amqp_sender_ex.publish(Message(body), routing_key=key, delivery_mode=DeliveryMode.PERSISTENT)

    async def _handler(self, source: str, port: int, message: aiosnmp.SnmpV2TrapMessage) -> None:
        try:
            oid, value = next(((trap.oid, trap.value) for trap in message.data), (None, None))
            if not oid is None:
                ter_id, ter_type, ter_ip, ter_ampp_id, ter_ampp_type = next(((device['terId'], device['terType'], device['terIp'], device['amppId'],
                                                                              device['amppType']) for device in self.__devices if device['terIp'] == source), (None, None, None, None, None))
                snmp_object = next((mib for mib in receiving_mibs if mib.oid == oid), None)
                if not snmp_object is None and not ter_id is None:
                    await self._amqp_send(snmp_object.data, 'trap')
                    if snmp_object.codename in ['BarrierLoop1Status', 'BarrierLoop2Status']:
                        await self._amqp_send(snmp_object.data, 'loop')
                    elif snmp_object.codename in ['PaymentStatus', 'PaymentAmount', 'PaymentCardType', 'PaymentMoneyType']:
                        await self._amqp_send(snmp_object.data, 'payment')
                    elif snmp_object.codename == 'PaymentStatus':
                        await self._amqp_send(snmp_object.data, 'lostticket')

        except Exception as e:
            await self.__logger.error(e)

    async def _dispatch(self):
        await self._logger.info({self.name: self.status})
        trap_listener = aiosnmp.SnmpV2TrapServer(host=snmp_trap_host, port=snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

     # graceful shutdown
    def _handler(self):
        asyncio.ensure_future(self._amqp_sender_cnx.close())
        tasks = [t for t in asyncio.all_tasks() if t is not
                 asyncio.current_task()]
        [task.cancel() for task in tasks]
        self.eventloop.stop()

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._amqp_connect())
            asyncio.ensure_future(self._dispatch())
            self.eventloop.run_forever()
        except KeyboardInterrupt:
            self.eventloop.close()
        # finally:
