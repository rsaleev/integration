
import asyncio
import aiosnmp
from datetime import datetime
from aio_pika import connect, ExchangeType, Message
from snmp.objects import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import json
from config.configuration import snmp_trap_host, snmp_trap_port, integration_conn, amqp_host, amqp_port, amqp_user, amqp_password, sys_log


class AsyncSNMPReceiver:

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
            self.__dbconnector = await AsyncDBPool(size=10, name='trap_retranslator', conn=integration_conn, loop=self.eventloop)
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
                'traps', ExchangeType.FANOUT)
            modules = await self.__dbconnector.callproc('is_services_get', [None, 1, 1])
            falcon_module = next(module['enabled'] for module in modules if module['service'] == 'falcon')
            notifier_module = next(module['enabled'] for module in modules if module['service'] == 'notifier')
            if falcon_module:
                queue_falcon = await self.__amqp.sender_ch.declare_queue('falcon_trap', exclusive=True)
                queue_falcon.bind(self.__amqp_sender_ex)
            if notifier_module:
                notifier_queue = await self.__amqp.sender_ch.declare_queue('notifier_trap', exclusive=True)
                notifier_queue.bind(self.__amqp_sender_ex)
            self.__amqp_status = True
        except Exception as e:
            await self.__logger.error(e)
            self.__amqp_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict):
        body = json.dumps(data)
        await self.__amqp_sender_ex.publish(Message(body), routing_key='trap')

    async def _handler(self, source: str, port: int, message: aiosnmp.SnmpV2TrapMessage) -> None:
        oid, value = next((trap.oid, trap.value) for trap in message.data)
        ter_id, ter_type, ter_ip, ter_ampp_id, ter_ampp_type = next(((device['terId'], device['terType'], device['terIp'], device['amppId'],
                                                                      device['amppType']) for device in self.__devices if device['terIp'] == source), (None, None, None, None, None))
        trap_object = next(
            (mib for mib in receiving_mibs if mib.oid == oid), None)
        if not trap_object is None and not ter_id is None and not ter_type is None and not ter_ampp_id is None and not ter_ampp_type is None:
            if not value is None:
                trap_object.snmpvalue = value
                trap_object.ts = datetime.now().timestamp()
                asyncio.ensure_future(self._amqp_send(trap_object.get))
                # store in DB
                asyncio.ensure_future(
                    self.__dbconnector.callproc('wp_status_ins', None, [ter_id, ter_type, ter_ampp_id, ter_ampp_type, trap_object.codename, trap_object.snmpvalue, source]))

    async def _dispatch(self):
        trap_listener = aiosnmp.SnmpV2TrapServer(
            host=snmp_trap_host, port=snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

    def start(self):
        self.eventloop = asyncio.get_event_loop()
        asyncio.ensure_future(self._logger_activate())
        asyncio.ensure_future(self._sql_connect())
        asyncio.ensure_future(self._ampq_connect())
        self.eventloop.create_task(self.main())
        self.eventloop.run_until_complete(self._dispatch())

    def stop(self):
        self.__dbconnector.close()
        self.__amqp_sender_cnx.close()
        self.eventloop.close()
