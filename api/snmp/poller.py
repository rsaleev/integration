import asyncio
import aiosnmp
from aiosnmp.exceptions import SnmpErrorNoSuchName, SnmpTimeoutError, SnmpErrorResourceUnavailable
from datetime import datetime
from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
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
        self.__dbconnector_is: object = None

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

    # Connect and create exchange for SNMP Poller and SNMP Receiver messages

    async def _ampq_connect(self):
        try:
            asyncio.ensure_future(self.__logger.info('Establishing RabbitMQ connection'))
            while self.__amqp_cnx is None:
                self.__amqp_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop, timeout=5)
            else:
                self.__amqp_ch = await self.__amqp_cnx.channel()
                self.__amqp_ex = await self.__amqp_ch.declare_exchange('integration', ExchangeType.TOPIC)
                self.__amqp_status = True
                asyncio.ensure_future(self.__logger.info(f"Connected to:{self.__amqp_cnx}"))
        except Exception as e:
            asyncio.ensure_future(self.__logger.error(e))
            self.__amqp_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict, key: str):
        body = json.dumps(data).encode()
        await self.__amqp_ex.publish(Message(body=body, delivery_mode=DeliveryMode.PERSISTENT), routing_key=key)  #

    async def _sql_connect(self):
        try:
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_is.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
            return self
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self
        

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
                                if snmp_object.codename == "BarrierLoop1Status":
                                    await self._amqp_send(snmp_object.data, 'loop1')
                                elif snmp_object.codename == "BarrierLoop2Status":
                                    await self._amqp_send(snmp_object.data, 'loop2')
                                else:
                                    await self._amqp_send(snmp_object.data, 'status')
                    # # handle SNMP exceptions
                    except (SnmpErrorNoSuchName, SnmpErrorResourceUnavailable, ValueError) as e:
                        asyncio.ensure_future(self.__logger.error(f"{device} {e}"))
                        continue
                    except Exception as e:
                        asyncio.ensure_future(self.__logger.error(f"{device} {e}"))
                        continue
            await asyncio.sleep(snmp_polling)

    def run(self):
        self.eventloop = asyncio.get_event_loop()
        self.eventloop.run_until_complete(self._log_init())
        self.eventloop.run_until_complete(self._sql_connect())
        self.eventloop.run_until_complete(self._ampq_connect())
        if self.status:
            self.eventloop.run_until_complete(self._dispatch())
        else:
            # self.eventloop.stop()
            # self.eventloop.close()
            raise Exception(f"{self.name} not started. Check logs")
