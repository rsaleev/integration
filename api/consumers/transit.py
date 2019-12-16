from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
import aio_pika
from aio_pika import robust_connection, Message, ExchangeType, DeliveryMode, IncomingMessage
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json


@dataclass
class TransitEventListener:

    def __init__(self, modules, devices):
        self.__amqp_sender_entry_cnx: object = None
        self.__amqp_sender_entry_ch: object = None
        self.__amqp_sender_entry_ex: object = None
        self.__amqp_sender_exit_cnx: object = None
        self.__amqp_sender_exit_ch: object = None
        self.__amqp_sender_exit_ex: object = None
        self.__amqp_reciever_cnx: object = None
        self.__amqp_reciever_ch: object = None
        self.__amqp_recevier_ex: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_sender_status = bool
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'TransitsListener'
        self.__modules = modules
        self.__devices = devices

    @dataclass
    class Entry:
        def __init__(self, ticket_number, tra_type, car_plate, start_time, terminal_id, payment):
            self.__ticket: int = ticket_number
            self.__tratype: int = tra_type
            self.__direction: str = 'I'
            self.__car_plate: str = car_plate
            self.__start_time: datetime = start_time.timestamp()
            self.__terminal_id: int = terminal_id
            self.__payment: int = payment

        @property
        def instance(self):
            data = {'ticket': self.__ticket, 'tra_type': self.__tratype, 'car_plate': self.__car_plate, 'start_time': self.__start_time,
                    'terminal_id': self.__terminal_id, 'payment': self.__payment}
            return data

    @dataclass
    class Exit:
        def __init__(self, ticket_number, tra_type, car_plate, stop_time, terminal_id, payment, discount, discount_type):
            self.__ticket: int = ticket_number
            self.__tratype: int = tra_type
            self.__direction: str = 'O'
            self.__car_plate: str = car_plate
            self.__stop_time: datetime = stop_time.timestamp()
            self.__terminal_id: int = terminal_id
            self.__payment: int = payment
            self.__discount: int = discount
            self.__discount_type: str = discount_type

        @property
        def instance(self):
            data = {'ticket': self.__ticket, 'tra_type': self.__tratype, 'car_plate': self.__car_plate, 'stop_time': self.__stop_time,
                    'terminal_id': self.__terminal_id, 'payment': self.__payment, 'discount': self.__discount, 'discount_type': self.__discount_type}
            return data

    @property
    def eventloop(self):
        return self.__loop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def status(self):
        if self.__sql_status and self.__amqp_receiver_status and self.__amqp_sender_status:
            return True
        else:
            return False

    async def _logger_activate(self):
        self.__logger = AsyncLogger().getlogger(sys_log)
        return self

    async def _sql_connect(self):
        try:
            self._db_connector_wp = await AsyncDBPool(conn=wp_cnx, loop=self.eventloop).connect()
            self._db_connector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self._db_connector_wp.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    # to implement fail resistant producer declare different connections and exchanges
    async def _sender_entry_connect(self, loop):
        try:
            self.__amqp_sender_entry_cnx = await robust_connection.connect(host=amqp_host, login=amqp_user, password=amqp_password, loop=loop)
            self.__amqp_sender_entry_ch = await self.__amqp_sender_entry_cnx.channel()
            self.__aqmp_sender_entry_ex = await self.__amqp_sender_entry_ch.declare_exchange('entries', ExchangeType.DIRECT)
            if 'falcon' in self.__modules:
                queue_falcon = await self.__amqp.sender_entry_ch.declare_queue('falcon_entries', exclusive=True)
                queue_falcon.bind(self.__amqp_sender_entry_ex, routing_key='entry')
            if 'ampp' in self.__modules:
                ampp_queue = await self.__amqp.sender_entry_ch.declare_queue('ampp_entries', exclusive=True)
                ampp_queue.bind(self.__amqp_sender_entry_ex, routing_key='entry')
            if 'epp' in self.__modules:
                ampp_queue = await self.__amqp.sender_entry_ch.declare_queue('epp_transits', exclusive=True)
                ampp_queue.bind(self.__amqp_sender_entry_ex, routing_key='entry')
            self._amqp_sender_status = True
        except Exception as e:
            self._amqp_sender_status = False
        finally:
            return self

    # for durabilty declare another connection and exchange for exit events
    async def _sender_exit_connect(self, loop):
        try:
            self.__amqp_sender_exit_cnx = await robust_connection.connect(host=amqp_host, login=amqp_user, password=amqp_password, loop=loop)
            self.__amqp_sender_exit_ch = await self.__amqp_sender_exit_cnx.channel()
            self.__aqmp_sender_exit_ex = await self.__amqp_sender_exit_ch.declare_exchange('entries', ExchangeType.DIRECT)
            if 'falcon' in self.__modules:
                queue_falcon = await self.__amqp.sender_exit_ch.declare_queue('falcon_exits', exclusive=True)
                queue_falcon.bind(self.__amqp_sender_exit_ex, routing_key='exit')
            if 'ampp' in self.__modules:
                ampp_queue = await self.__amqp.sender_exit_ch.declare_queue('ampp_exits', exclusive=True)
                ampp_queue.bind(self.__amqp_sender_exit_ex, rouitng_key='exit')
            if 'epp' in self.__modules:
                epp_queue = await self.__amqp.sender_exit_ch.declare_queue('epp_exits', exclusive=True)
                epp_queue.bind(self.__amqp_sender_exit_ex, routing_key='exit')
            self._amqp_sender_status = True
        except Exception as e:
            self._amqp_sender_status = False
        finally:
            return self

    async def _receiver_connect(self):
        try:
            self.__amqp_receiver_cn = await robust_connection.connect(host=amqp_host, login=amqp_user, password=amqp_password, loop=self.eventloop)
            self.__amqp_receiver_ch = await self.__amqp_reciver_cn.channel()
            self.__amqp_receiver_ex = await self.__amqp_reciever_ch.declare_exchange('snmp_trap')
            self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('loop_trap', exclusive=True)
            self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('payment')
            await self.__amqp_receiver_q.bind(self._amqp.receiver_ex, routing_key='loop_trap')
            self.__amqp_receiver_status = True
            return self
        except:
            self.__amqp_receiver_status = False
            raise

    async def _receiver_on_message(self, message: IncomingMessage):
        with message.process():
            message = json.loads(message.body.decode())
            return message

    async def _amqp_send_entry(self, data: dict):
        body = json.dumps(data).encode()
        await self.__amqp_sender_entry_ex.publish(Message(body), routing_key='entry', delivery_mode=DeliveryMode.PERSISTENT)

    async def _amqp_send_exit(self, data: dict):
        body = json.dumps(data).encode()
        await self.__amqp_sender_entry_ex.publish(Message(body), routing_key='exit', delivery_mode=DeliveryMode.PERSISTENT)

    async def _dispatch(self):
        try:
            message = await self._amqp_receiver_q.consume(self._receiver_on_message)
            if message['codename'] == 'BarrierLoop2Status' and message['snmpvalue'] == 1:
                if message['device_type'] == 1:
                    records = await self._db_connector_wp.callproc('wp_transits_get', [message['ts'], 1, 1])
                    if not records is None:
                        for record in records:
                            data = self.Entry(record['transitionTicket'], record['transitionPlate'],
                                              record['transitionTS'], message['amppId'], record['transitionType'], record['transitionPayments']).instance
                            asyncio.ensure_future(self._amqp_send_entry(data), self.eventloop)
                elif message['device_type'] == 2:
                    records = await self._db_connector_wp.callproc('wp_transits_get', [message['ts'], 2, 1])
                    if not records is None:
                        for record in records:
                            data = self.Exit(record['transitionTicket'], record['transitionType'], record['transitionPlate'], record['transitionTS'],
                                             message['amppId'], record['transitionPayments'], record['transitionDiscount'], record['transitionPaytype']).instance
                            asyncio.ensure_future(self._amqp_send_exit(data), self.eventloop)
                # check if trigger was on lost ticket entry event
                elif message['codename'] == 'PaymentStatus':
                    records = await self._db_connector_wp.callproc('wp_transits_get', [message['ts'], 1, 1])
                    if not records is None:
                        for record in records:
                            data = self.Entry(record['transitionTicket'], record['transitionPlate'],
                                              record['transitionTS'], message['amppId'], record['transitionType'], record['transitionPayments']).instance
                            asyncio.ensure_future(self._amqp_send_entry(data), self.eventloop)
        except Exception as e:
            await self.__logger.error(e)

    # graceful shutdown
    def _handler(self):
        self.loop.runself.__dbconnector_is.pool.close()
        self.__dbconnector_wp.close()
        asyncio.ensure_future(self.__amqp_sender_entry_cnx.close())
        asyncio.ensure_future(self.__amqp_sender_exit_cnx.close())
        asyncio.ensure_future(self.__dbconnector_wp.wait_closed())
        self.eventloop.close()

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            #self.eventloop.add_signal_handler(signal.SIGTERM, self._handler)
            #self.eventloop.add_signal_handler(signal.SIGINT, self._handler)
            #self.eventloop.add_signal_handler(signal.SIGKILL, self._handler)
            self.eventloop.run_until_complete(self._logger_activate())
            self.eventloop.run_until_complete(self._sql_connect())
            self.eventloop.run_until_complete(self._receiver_connect())
            self.eventloop.run_until_complete(self._sender_entry_connect())
            self.eventloop.run_until_complete(self._sender_exit_connect())
            asyncio.ensure_future(self._dispatch())
        except Exception as e:
            # self.eventloop.run_until_complete(self.__logger.error(e))
            print(e)
            # self.eventloop.close()
