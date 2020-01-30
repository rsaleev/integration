import asyncio
import aiosnmp
from aiosnmp.exceptions import SnmpErrorNoSuchName, SnmpTimeoutError, SnmpErrorResourceUnavailable
from datetime import datetime
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
from utils.asyncamqp import AsyncAMQP
import json
from configuration import snmp_retries, snmp_timeout, snmp_polling, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
from .mibs import polling_mibs
import signal


class AsyncSNMPPoller:

    def __init__(self, devices_l):
        self.__amqp_cnx: object = None
        self.__amqp_ch: object = None
        self.__amqp_ex: object = None
        self.__eventloop = None
        self.__logger = None
        self.__amqp_status = bool
        self.name = 'SNMPPoller'
        self.__devices = devices_l
        self.__amqpconnector = None

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info({"module": self.name, "info": "Logging initialized"})
        return self.__logger

    # Connect and create exchange for SNMP Poller and SNMP Receiver messages
    async def _amqp_connect(self):
        self.__amqpconnector = await AsyncAMQP(self.eventloop, user=amqp_user, password=amqp_password, host=amqp_host, exchange_name='integration', exchange_type='topic').connect()
        return self.__amqpconnector

    async def _dispatch(self):
        while True:
            for device in self.__devices:
                with aiosnmp.Snmp(host=device['terIp'], port=161, community="public", timeout=snmp_timeout, retries=snmp_retries) as snmp:
                    try:
                        for res in await snmp.get([mib.oid for mib in polling_mibs]):
                            snmp_object = next((mib for mib in polling_mibs if mib.oid == res.oid), None)
                            if not snmp_object is None:
                                snmp_object.ts = datetime.now().timestamp()
                                snmp_object.device_id = device['terId']
                                snmp_object.device_type = device['terType']
                                snmp_object.ampp_id = device['amppId']
                                snmp_object.ampp_type = device['amppType']
                                snmp_object.device_ip = device['terIp']
                                snmp_object.snmpvalue = res.value
                                await self.__logger.debug(snmp_object.data)
                                if snmp_object.codename == "BarrierLoop1Status":
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.loop1')
                                elif snmp_object.codename == "BarrierLoop2Status":
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.loop2')
                                else:
                                    await self.__amqpconnector.send(snmp_object.data, persistent=True, key='status.snmp')
                            await asyncio.sleep(0.1)
                    # handle SNMP exceptions
                    except (SnmpErrorNoSuchName, SnmpErrorResourceUnavailable, ValueError, SnmpTimeoutError) as e:
                        await asyncio.sleep(0.1)
                        pass
                    except BaseException as e:
                        asyncio.ensure_future(self.__logger.error({"module": self.name, "exception": repr(e)}))
                        await asyncio.sleep(0.1)
                        pass
            await asyncio.sleep(snmp_polling)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._amqp_connect())
        self.eventloop.run_until_complete(self._dispatch())
