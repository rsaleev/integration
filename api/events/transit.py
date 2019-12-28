from datetime import datetime
import asyncio
import signal
from dataclasses import dataclass
import aio_pika
from aio_pika import connect_robust, Message, ExchangeType, DeliveryMode, IncomingMessage
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json


@dataclass
class TransitEventListener:

    def __init__(self, modules_l: list, devices_l: list):
        self.__amqp_sender_entry_cnx: object = None
        self.__amqp_sender_entry_ch: object = None
        self.__amqp_sender_entry_ex: object = None
        self.__amqp_sender_exit_cnx: object = None
        self.__amqp_sender_exit_ch: object = None
        self.__amqp_sender_exit_ex: object = None
        self.__amqp_receiver_cnx: object = None
        self.__amqp_receiver_ch: object = None
        self.__amqp_recevier_ex: object = None
        self.__amqp_receiver_q: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_sender_status = bool
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'TransitsListener'
        self.__modules = modules_l
        self.__devices = devices_l

    @dataclass
    class Entry:
        def __init__(self, ticket_number, tra_type, car_plate, start_time, terminal_id, terminal_type, payment):
            self.__ticket: int = ticket_number
            self.__tratype: int = tra_type
            self.__direction: str = 'I'
            self.__car_plate: str = car_plate
            self.__start_time: datetime = int(start_time.timestamp())
            self.__terminal_id: int = terminal_id
            self.__terminal_type: int = terminal_type
            self.__payment: int = payment

        @property
        def instance(self):
            data = {'ticket': self.__ticket, 'tra_type': self.__tratype, 'car_plate': self.__car_plate, 'start_time': self.__start_time,
                    'terminal_id': self.__terminal_id, 'terminal_type': self.__terminal_type, 'payment': self.__payment}
            return data

    @dataclass
    class Exit:
        def __init__(self, ticket_number, tra_type, car_plate, stop_time, terminal_id, terminal_type, payment, discount, discount_type):
            self.__ticket: int = ticket_number
            self.__tratype: int = tra_type
            self.__direction: str = 'O'
            self.__car_plate: str = car_plate
            self.__stop_time: datetime = int(stop_time.timestamp())
            self.__terminal_id: int = terminal_id  # AMPP ID
            self.__terminal_type: int = terminal_type  # AAMPP type
            self.__payment: int = payment  # For CMIU and EPP
            self.__discount: int = discount  # For CMIU and EPP
            self.__discount_type: str = discount_type  # For CMIU and EPP

        @property
        def instance(self):
            data = {'ticket': self.__ticket, 'tra_type': self.__tratype, 'car_plate': self.__car_plate, 'stop_time': self.__stop_time,
                    'ampp_id': self.__terminal_id, 'ampp_type': self.__terminal_type, 'payment': self.__payment, 'discount': self.__discount, 'discount_type': self.__discount_type}
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

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self.__logger

    async def _sql_connect(self):
        try:
            self.__dbconnector_wp = await AsyncDBPool(conn=wp_cnx, loop=self.eventloop).connect()
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_wp.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    # to implement fail resistant producer declare different connections and exchanges
    async def _sender_entry_connect(self):
        try:
            while self.__amqp_sender_entry_cnx is None:
                self.__amqp_sender_entry_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}", loop=self.eventloop)
            else:
                self.__amqp_sender_entry_ch = await self.__amqp_sender_entry_cnx.channel()
                self.__aqmp_sender_entry_ex = await self.__amqp_sender_entry_ch.declare_exchange('entries', ExchangeType.DIRECT)
                if 'falcon' in self.__modules:
                    queue_falcon = await self.__amqp_sender_entry_ch.declare_queue('falcon_entries', exclusive=True)
                    queue_falcon.bind(self.__amqp_sender_entry_ex, routing_key='entry')
                if 'ampp' in self.__modules:
                    ampp_queue = await self.__amqp_sender_entry_ch.declare_queue('ampp_entries', exclusive=True)
                    ampp_queue.bind(self.__amqp_sender_entry_ex, routing_key='entry')
                if 'epp' in self.__modules:
                    ampp_queue = await self.__amqp_sender_entry_ch.declare_queue('epp_entries', exclusive=True)
                    ampp_queue.bind(self.__amqp_sender_entry_ex, routing_key='entry')
                self.__amqp_sender_status = True
        except Exception as e:
            self.__amqp_sender_status = False
            await self.__logger.error(e)
        finally:
            return self

    # for durabilty declare another connection and exchange for exit events
    async def _sender_exit_connect(self):
        try:
            while self.__amqp_sender_exit_cnx is None:
                self.__amqp_sender_exit_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}", loop=self.eventloop)
            else:
                self.__amqp_sender_exit_ch = await self.__amqp_sender_exit_cnx.channel()
                self.__aqmp_sender_exit_ex = await self.__amqp_sender_exit_ch.declare_exchange('exits', ExchangeType.DIRECT)
                if 'falcon' in self.__modules:
                    queue_falcon = await self.__amqp_sender_exit_ch.declare_queue('falcon_exits', exclusive=True)
                    queue_falcon.bind(self.__amqp_sender_exit_ex, routing_key='exit')
                if 'ampp' in self.__modules:
                    ampp_queue = await self.__amqp_sender_exit_ch.declare_queue('ampp_exits', exclusive=True)
                    ampp_queue.bind(self.__amqp_sender_exit_ex, rouitng_key='exit')
                if 'epp' in self.__modules:
                    epp_queue = await self.__amqp_sender_exit_ch.declare_queue('epp_exits', exclusive=True)
                    epp_queue.bind(self.__amqp_sender_exit_ex, routing_key='exit')
                self.__amqp_sender_status = True
        except Exception as e:
            self.__amqp_sender_status = False
        finally:
            return self

    async def _receiver_connect(self):
        try:
            while self.__amqp_receiver_cnx is None:
                self.__amqp_receiver_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop)
            else:
                self.__amqp_receiver_ch = await self.__amqp_receiver_cnx.channel()
                self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('statuses', ExchangeType.DIRECT)
                self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('integration')
                await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='loop2')
                await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='payment')
                self.__amqp_receiver_status = True
        except:
            self.__amqp_receiver_status = False
            raise
        finally:
            return self

    async def _amqp_send(self, data: dict, key):
        body = json.dumps(data).encode()
        await self.__amqp_sender_entry_ex.publish(Message(body), routing_key=key, delivery_mode=DeliveryMode.PERSISTENT)

    async def _dispatch(self):
        while True:
            try:
                async with self.__amqp_receiver_q.iterator() as q:
                    async for message in q:
                        data = json.loads(message.body.decode())
                        if (message['codename'] == 'BarrierLoop2Status' and message['snmpvalue'] == 1):
                            if message['device_type'] == 1:
                                record = await self._db_connector_wp.callproc('wp_transits_get', rows=1, values=[1, 1, message['device_id']])
                                if not record is None:
                                    data = self.Entry(record['transitionTicket'], record['transitionPlate'],
                                                      record['transitionTS'], message['ampp_id'], message['ampp_type'], record['transitionType'], record['transitionPayments']).instance
                                    asyncio.ensure_future(self._amqp_send_entry(data), 'entry')
                            elif message['device_type'] == 2:
                                record = await self._dbconnector_wp.callproc('wp_transits_get', rows=1, values=[2, 1, message['device_id']])
                                if not record is None:
                                    data = self.Exit(record['transitionTicket'], record['transitionType'], record['transitionPlate'], record['transitionTS'],
                                                     message['ampp_id'],  message['ampp_type'], record['transitionPayments'], record['transitionDiscount'], record['transitionPaytype']).instance
                                    asyncio.ensure_future(self._amqp_send_exit(data), 'exit')
                        # check if trigger was on lost ticket entry event
                        elif message['codename'] == 'PaymentStatus':
                            records = await self._db_connector_wp.callproc('wp_transits_get', rows=-1, values=[message['ts'], 1, 1, message['device_id']])
                            if not records is None:
                                for record in records:
                                    data = self.Entry(record['transitionTicket'], record['transitionPlate'],
                                                      record['transitionTS'], message['ampp_id'],  message['ampp_type'], record['transitionType'], record['transitionPayments']).instance
                                    asyncio.ensure_future(self._amqp_send_entry(data), 'entry')
            except Exception as e:
                await self.__logger.error(e)
                continue

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._sql_connect())
            self.eventloop.run_until_complete(self._receiver_connect())
            self.eventloop.run_until_complete(self._sender_entry_connect())
            self.eventloop.run_until_complete(self._sender_exit_connect())
            self.eventloop.run_until_complete(self._dispatch())
        except (KeyboardInterrupt, SystemExit):
            [task.cancel() for task in asyncio.Task.all_tasks() if not task.done()]
            # asyncio.ensure_future(self.__amqp_receiver_cnx.close())
            # asyncio.ensure_future(self.__amqp_sender_entry_cnx.close())
            # asyncio.ensure_future(self.__amqp_sender_exit_cnx.close())
            # self.eventloop.run_in_executor(self.__dbconnector_is.pool.close())
            # self.eventloop.run_in_executore(self.__dbconnector_wp.pool.close())
            # asyncio.ensure_future(self.__dbconnector_is.pool.wait_closed())
            # asyncio.ensure_future(self.__dbconnector_wp.pool.wait_closed())
            self.eventloop.run_until_complete(asyncio.sleep(0))
            self.eventloop.stop()
            self.eventloop.close()
