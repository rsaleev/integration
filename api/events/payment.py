from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from utils.asyncsoap import AsyncSOAP
import configuration as cfg
import asyncio
import datetime
import json
import uuid


class PaymentListener:
    def __init__(self, device_l: list):
        self.__dbconnector_wp: object = None
        self.__soapconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__loop: object = None
        self.__devices = device_l
        self.name = 'PlacesListener'

    @property
    def eventloop(self):
        return self.__loop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'info': 'Logging initialized'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Integration RDBS Pool Connection'})
        self.__dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
        await self.__logger.info({'module': self.name, 'info': f'Integration RDBS Connection: {self.__dbconnector_is.connected}'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        await self.__logger.info({'module': self.name, 'info': f'Wisepark RDBS Connection: {self.__dbconnector_wp.connected}'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection'})
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        await self.__amqpconnector.bind('statuses', ['status.autocash.payment'])
        await self.__logger.info({'module': self.name, 'info': f'AMQP Connection: {self.__amqpconnector.connected}'})
        return self

    # callback

    async def _process(self, redelivered, key, data):
        if not redelivered:
            if data['value'] in ['FINISHED_WITH_SUCCESS', 'FINISHED_WITH_ISSUES']:
                payment_data = await self.__dbconnector_wp.callproc('wp_payment_get', rows=1, values=[data['terminal_id']])
                msg = {'transacation_uid': data['tra_uid'],
                       'data': payment_data}
