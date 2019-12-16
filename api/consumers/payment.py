import asyncio
from dataclasses import dataclass
import datetime
import aio_pika
from aio_pika import robust_connection, Message, ExchangeType, DeliveryMode, IncomingMessage
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from configuration import wp_cnx, is_cnx, sys_log, amqp_host, amqp_password, amqp_user
import json

"""
Class for receiving SNMP triggers:
- payment 

Generates events data:
- payment
- money quantity change

"""


class PaymentsListener:

    def __init__(self, modules, devices):
        self.__amqp_sender_payment_cnx: object = None
        self.__amqp_sender_payment_ch: object = None
        self.__amqp_sender_payment_ex: object = None
        self.__amqp_sender_money_cnx: object = None
        self.__amqp_sender_money_ch: object = None
        self.__amqp_sender_money_ex: object = None
        self.__amqp_receiver_cnx: object = None
        self.__amqp_receiver_ch: object = None
        self.__amqp_recevier_ex: object = None
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__amqp_sender_status = bool
        self.__amqp_receiver_status = bool
        self.__sql_status = bool
        self.name = 'PaymentsListener'
        self.__modules = modules
        self.__devices = devices
    """ 
    class for representing Payment event
    """

    @dataclass
    class Payment:
        def __init__(self, uid: int, ts: datetime, ticket: int, subscription: int, tra_type: str, terminal_id: int, amount: int, payed: int, change: int, nochange: int, pay_type: str, discount_val: int, discount: int):
            self.__uid = uid
            self.__ts = datetime
            self.__ticket = ticket
            self.__subscription = subscription
            self.__tra_type = tra_type
            self.__terminal_id = terminal_id
            self.__amount = amount
            self.__payed = payed
            self.__change = change
            self.__nochange = nochange
            self.__pay_type = pay_type
            self.__discount_val = discount_val
            self.__discount = discount

        @property
        def instance(self):
            return {'uid': self.__uid, 'transition': self.__tra_type, 'ticket': self.__ticket, 'subscription': self.__subscription, 'terminal': self.__terminal_id,
                    'amount': self.__amount, 'paid': self.__payed, 'change': self.__change, 'nochange': self.__nochange, 'payment': self.__pay_type,
                    }

    @dataclass
    class Money:
        def __init__(self, terminal_id: int, channel: int, value: int, quantity: int, amount: int, ts: datetime):
            self.__terminalid = terminal_id
            self.__channel = channel
            self.__value = value
            self.__quantity = quantity
            self.__amount = amount
            self.__ts = datetime

        @property
        def ts(self):
            return self.__ts

        @ts.setter
        def ts(self, value):
            self.__ts = value

        @ts.getter
        def ts(self):
            return self.ts.timestamp()

        @property
        def channel(self):
            return self.__channel

        @channel.setter
        def channel(self, value):
            self.__channel = value

        @channel.getter
        def channel(self):
            if channel == 1:
                return 'payout'
            elif channel == 2:
                return 'cashbox'

        @property
        def instance(self):
            return {'terminalid': self.__terminalid, 'channel': self.channel, 'value': self.__value, 'quantity': self.__quantity, 'amount': self.__amount, 'ts': self.ts}

    async def _log_init(self):
        self.__logger = await AsyncLogger().getlogger(sys_log)
        await self.__logger.info(f'Module {self.name}. Logging initialized')
        return self

    async def _sender_payment_connect(self, loop):
        try:
            self.__amqp_sender_payment_cnx = await robust_connection.connect(host=amqp_host, login=amqp_user, password=amqp_password, loop=self.eventloop)
            self.__amqp_sender_payment_ch = await self.__amqp_sender_payment_cnx.channel()
            self.__aqmp_sender_payment_ex = await self.__amqp_sender_payment_ch.declare_exchange('payments', ExchangeType.DIRECT)
            if 'ampp' in self.__modules:
                ampp_queue = await self.__amqp.sender_exit_ch.declare_queue('ampp_payments', exclusive=True)
                ampp_queue.bind(self.__amqp_sender_exit_ex, rouitng_key='payment')
            if 'epp' in self.__modules:
                epp_queue = await self.__amqp.sender_exit_ch.declare_queue('epp_payments', exclusive=True)
                epp_queue.bind(self.__amqp_sender_exit_ex, routing_key='payment')
            self._amqp_sender_status = True
        except Exception as e:
            self._amqp_sender_status = False
        finally:
            return self

    async def _amqp_send_payment(self, data: dict):
        body = json.dumps(data).encode()
        await self.__amqp_sender_payment_ex.publish(Message(body), routing_key='payment', delivery_mode=DeliveryMode.PERSISTENT)

    async def _amqp_send_money(self, data: dict):
        body = json.dumps(data).encode()
        await self.__amqp_sender_money_ex.publish(Message(body), routing_key='money', delivery_mode=DeliveryMode.PERSISTENT)

    async def _receiver_connect(self):
        try:
            self.__amqp_receiver_cn = await robust_connection.connect(host=amqp_host, login=amqp_user, password=amqp_password, loop=self.eventloop)
            self.__amqp_receiver_ch = await self.__amqp_receiver_cn.channel()
            self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('snmp_trap')
            self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('is_payment_trap')
            await self.__amqp_receiver_q.bind(self._amqp.receiver_ex, routing_key='payment')
            self.__amqp_receiver_status = True
            return self
        except:
            self.__amqp_receiver_status = False
            raise

    async def _receiver_on_message(self, message: IncomingMessage):
        with message.process():
            message = json.loads(message.body.decode())
            return message

    async def _sql_connect(self):
        try:
            self.__dbconnector_wp = await AsyncDBPool(conn=wp_cnx, loop=self.eventloop).connect()
            self.__dbconnector_is = await AsyncDBPool(conn=is_cnx, loop=self.eventloop).connect()
            if self.__dbconnector_wp.connected and self.__dbconnector_is.connected:
                self.__sql_status = True
            else:
                self.__sql_status = False
        except Exception as e:
            self.__sql_status = False
            await self.__logger.error(e)
        finally:
            return self

    async def _dispatch(self):
        try:
            message = await self._amqp_receiver_q.consume(self._receiver_on_message)
            if message['codename'] == 'PaymentStatus':
                payments = self._db_connector_wp.callproc('wp_payment_get', None, [message['ts'], message['device_id']])
                if not payments is None:
                    for record in payments:
                        # uid: int, ts: datetime, ticket: int, subscription: int, tra_type: str, terminal_id: int, amount: int, payed: int, change: int, nochange: int, pay_type: str, discount_val: int, discount: int
                        data = self.Payment(record['payUID'], record['payCreation'].timestamp(), record['tidTraKey'], record['paySubscription'], record['payTraType'], message['ampp_id'], record['payAmount'],
                                            record['payPayed'], record['payChange'], record['payNoChange'], record['payType'], record['payDiscountValue'], record['payDiscount']).instance
                        asyncio.ensure_future(self._amqp_send_payment(data))
                inventories = self._db_connector_wp.callproc('wp_money_get', None, [message['ts'], message['device_id']])
                if not inventories is None:
                    for record in inventories:
                        money = self.Money(record['curTerId'], record['curChannelId'], record['curValue'], record['curQuantity'], record['curAmount'], message['ts'])
                        asyncio.ensure_future(self._db_connector_is.callproc('wp_money_ins', None, [money.terminalid, message['amppId'], money.channel, money.value, money.quantity, money.ts]))
        except Exception as e:
            await self.__logger.error(e)

      # graceful shutdown

    async def _handler(self):
        # self.loop.runself.__dbconnector_is.pool.close()
        self.__dbconnector_wp.close()
        asyncio.ensure_future(self.__amqp_sender_entry_cnx.close())
        asyncio.ensure_future(self.__amqp_sender_exit_cnx.close())
        asyncio.ensure_future(self.__dbconnector_wp.wait_closed())
        self.eventloop.close()

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._sql_connect())
            self.eventloop.run_until_complete(self._receiver_connect())
            self.eventloop.run_until_complete(self._sender_entry_connect())
            self.eventloop.run_until_complete(self._sender_exit_connect())
            asyncio.ensure_future(self._dispatch())
        except Exception as e:
            # self.eventloop.run_until_complete(self.__logger.error(e))
            print(e)
            # self.eventloop.close()
