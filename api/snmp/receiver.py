
import asyncio
import aiosnmp
from datetime import datetime
import json
import configuration as cfg
from .mibs import receiving_mibs
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP


class AsyncSNMPReceiver:

    def __init__(self, devices):
        self.__amqpconnector = None
        self.__eventloop = None
        self.__logger = None
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
        return self.__amqpconnector.connected

    @property
    def name(self):
        return self.__name

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _amqp_connect(self):
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection Status'})
        self.__amqpconnector = await AsyncAMQP(self.eventloop, user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__logger.info({'module': self.name, 'AMQP Connection Status': self.__amqpconnector.connected})
        return self

    async def _handler(self, host: str, port: int, message: aiosnmp.SnmpV2TrapMessage):
        try:
            oid, value = next(((trap.oid, trap.value) for trap in message.data.varbinds), (None, None))
            if not oid is None:
                ter_id, ter_type, ter_ip, ter_ampp_id, ter_ampp_type = next(((device['terId'], device['terType'], device['terIp'], device['amppId'],
                                                                              device['amppType']) for device in self.__devices if device['terIp'] == host), (None, None, None, None, None))
                snmp_object = next((mib for mib in receiving_mibs if mib.oid == oid), None)
                if not snmp_object is None and not ter_id is None:
                    snmp_object.ts = datetime.now().timestamp()
                    snmp_object.device_id = ter_id
                    snmp_object.device_type = ter_type
                    snmp_object.ampp_id = ter_ampp_id
                    snmp_object.ampp_type = ter_ampp_type
                    snmp_object.device_ip = host
                    await self.__logger.info(snmp_object.data)
                    if snmp_object.codename == 'BarrierLoop1Status':
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.loop1', priority=10)
                    elif snmp_object.codename == 'BarrierLoop2Status':
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.loop2', priority=10)
                    else:
                        await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.trap', priority=9)
        except Exception as e:
            await self.__logger.error(e)
            await asyncio.sleep(0.2)

    async def _dispatch(self):
        await self.__logger.info({self.name: self.status})
        trap_listener = aiosnmp.SnmpV2TrapServer(host=cfg.snmp_trap_host, port=cfg.snmp_trap_port, communities=("public",), handler=self._handler)
        await trap_listener.run()

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
