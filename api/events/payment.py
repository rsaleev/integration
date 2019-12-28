import asyncio
from dataclasses import dataclass
import datetime
import aio_pika
from aio_pika import connect_robust, Message, ExchangeType, DeliveryMode, IncomingMessage
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

    def __init__(self, modules_l, devices_l):
        self.__amqp_sender_cnx: object = None
        self.__amqp_sender_ch: object = None
        self.__amqp_sender_ex: object = None
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
        self.name = 'PaymentsListener'
        self.__modules = modules_l

    @property
    def status(self):
        if self.__sql_status and self.__amqp_receiver_status and self.__amqp_sender_status:
            return True
        else:
            return False

    """
    class for representing Payment event
    """

    @dataclass
    class Payment:
        def __init__(self, uid: int, ts: datetime, ticket: int, subscription: int, tra_type: str, terminal_id: int, amount: int, payed: int, count: int, change: int, nochange: int, pay_type: str, discount_val: int, discount: int, carplate: str):
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
            self.__pay_count = count
            self.__discount_val = discount_val
            self.__discount = discount
            self.__carplate = carplate

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
        def instance(self):
            return {'uid': self.__uid, 'transition': self.__tra_type, 'ticket': self.__ticket, 'subscription': self.__subscription, 'terminal': self.__terminal_id,
                    'amount': self.__amount, 'paid': self.__payed, 'change': self.__change, 'nochange': self.__nochange, 'count': self.__pay_count, 'type': self.__pay_type,
                    'payment_time': self.__ts, 'discount': self.__discount_val, 'plate': self.__carplate}

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

    async def _sender_connect(self):
        try:
            while self.__amqp_sender_cnx is None:
                self.__amqp_sender_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop, timeout=5)
            else:
                self.__amqp_sender_ch = await self.__amqp_sender_cnx.channel()
                self.__amqp_sender_ex = await self.__amqp_sender_ch.declare_exchange('payments', ExchangeType.DIRECT)
                if 'ampp' in self.__modules:
                    # 1st queue with prepared payment data
                    ampp_queue_payments = await self.__amqp_sender_ch.declare_queue('ampp_payments')
                    ampp_queue_payments.bind(self.__amqp_sender_ex, rouitng_key='payment')
                    # 2nd queue for triggering check Integration DB for new records from inventory status
                    ampp_queue_money = await self.__amqp_sender_ch.declare_queue('ampp_money')
                    ampp_queue_money.bind(self.__amqp_sender_ex, rouitng_key='money')
                if 'epp' in self.__modules:
                    epp_queue = await self.__amqp_sender_ch.declare_queue('epp_payments')
                    epp_queue.bind(self.__amqp_sender_ex, routing_key='payment')
                self._amqp_sender_status = True
                asyncio.ensure_future(self.__logger.info(f"RabbitMQ producer connected to:{self.__amqp_receiver_cnx}"))
        except Exception as e:
            asyncio.ensure_future(self.__logger.error(e))
            self._amqp_sender_status = False
        finally:
            return self

    async def _amqp_send(self, data: dict, key: str):
        body = json.dumps(data).encode()
        await self.__amqp_sender_payment_ex.publish(Message(body), routing_key=key, delivery_mode=DeliveryMode.PERSISTENT)

    async def _receiver_connect(self):
        try:
            while self.__amqp_receiver_cnx is None:
                self.__amqp_receiver_cnx = await connect_robust(f"amqp://{amqp_user}:{amqp_password}@{amqp_host}/", loop=self.eventloop, timeout=5)
            else:
                self.__amqp_receiver_ch = await self.__amqp_receiver_cnx.channel()
                self.__amqp_receiver_ex = await self.__amqp_receiver_ch.declare_exchange('statuses')
                self.__amqp_receiver_q = await self.__amqp_receiver_ch.declare_queue('payment', durable=True)
                await self.__amqp_receiver_q.bind(self.__amqp_receiver_ex, routing_key='payment')
                self.__amqp_receiver_status = True
                await self.__logger.info(f"RabbitMQ consumer connected to:{self.__amqp_receiver_cnx}")
        except Exception as e:
            await self.__logger.error(e)
            self.__amqp_receiver_status = False
            raise
        finally:
            return self

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
            asyncio.ensure_future(self.__logger.error(e))
        finally:
            return self

    async def _dispatch(self):
        while True:
            try:
                async with self.__amqp_receiver_q.iterator() as q:
                    async for message in q:
                        data = json.loads(message.body.decode())
                        if data['codename'] == 'PaymentStatus':
                            payment = self.__dbconnector_wp.callproc('wp_payment_get', rows=1, values=[data['device_id']])
                            if not payment is None and int(data['ts']) - payment['payCreation'] < 10:
                                payment_msg = self.Payment(payment['payUID'], payment['payCreation'], payment['tidTraKey'],
                                                           payment['paySubscription'], payment['payTraType'], data['ampp_id'], payment['payAmount'],
                                                           payment['payPayed'], payment['payChange'], payment['payNoChange'], payment['payCount'], payment['payType'],
                                                           payment['payDiscountValue'], payment['payDiscount'], payment['traPlate']).instance
                                body = json.dumps(payment_msg).encode()
                                asyncio.ensure_future(self.__amqp_sender_ex.publish(Message(body), routing_key='payment', delivery_mode=DeliveryMode.PERSISTENT))
                            inventory = self.__dbconnector_wp.callproc('wp_money_get', rows=-1, values=[data['device_id']])
                            for inv in inventory:
                                money = self.Money(inv['curTerId'], inv['curChannelId'], inv['curValue'], inv['curQuantity'], inv['curAmount'], data['ts'])
                                body = json.dumps(money.instance).encode()
                                asyncio.ensure_future(self.__dbconnector_is.callproc('wp_money_ins', rows=0, values=[
                                                      money.terminalid, data['amppId'], money.channel, money.value, money.quantity, money.ts]))
                                asyncio.ensure_future(self.__amqp_sender_ex.publish(Message(body), routing_key='money', delivery_mode=DeliveryMode.PERSISTENT))
            except Exception as e:
                asyncio.ensure_future(self.__logger.error(e))
                break

    def run(self):
        try:
            self.eventloop = asyncio.get_event_loop()
            self.eventloop.run_until_complete(self._log_init())
            self.eventloop.run_until_complete(self._sql_connect())
            self.eventloop.run_until_complete(self._receiver_connect())
            self.eventloop.run_until_complete(self._sender_connect())
            if self.status:
                self.eventloop.run_until_complete(self._dispatch())
        except:
            pass
        # except KeyboardInterrupt:
        #     [task.cancel() for task in asyncio.Task.all_tasks() if not task.done()]
        #     # self.eventloop.run_in_executor(self.__dbconnector_is.pool.close())
        #     # self.eventloop.run_in_executore(self.__dbconnector_wp.pool.close())
        #     # asyncio.ensure_future(self.__dbconnector_is.pool.wait_closed())
        #     # asyncio.ensure_future(self.__dbconnector_wp.pool.wait_closed())
        #     self.eventloop.run_until_complete(asyncio.sleep(0))
        #     self.eventloop.stop()
        #     self.eventloop.close()
