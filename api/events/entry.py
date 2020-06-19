
import asyncio
import base64
import functools
import json
import os
import signal
import sys
import uuid
from datetime import datetime, timedelta
from uuid import uuid4

import aiohttp
from bs4 import BeautifulSoup
from dateutil import parser as dp
from setproctitle import setproctitle

import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool


class EntryListener:
    def __init__(self):
        self.__dbconnector_ws: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector_is: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.name = 'EntryListener'

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
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v):
        return self.__eventsignal

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class OneShotEvent:
        def __init__(self, value):
            self.__codename = 'OneShotEvent'
            self.__value: str = value
            self.__device_id = 0
            self.__device_address = 0
            self.__device_ip = cs.WS_SERVER_IP
            self.__device_type = 0
            self.__ampp_id = int(f'{cs.AMPP_PARKING_ID}01')
            self.__ampp_type = 1
            self.__ts = datetime.now().timestamp()

        @property
        def instance(self):
            return {'device_id': self.__device_id,
                    'device_address': self.__device_address,
                    'device_type': self.__device_type,
                    'codename': self.__codename,
                    'value': self.__value,
                    'ts': self.__ts,
                    'ampp_id': self.__ampp_id,
                    'ampp_type': self.__ampp_type,
                    'device_ip': self.__device_ip}

    async def _initialize(self):
        setproctitle('integration-entries')
        self.__logger = await AsyncLogger().getlogger(cs.IS_LOG)
        try:
            await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
            connections_tasks = []
            connections_tasks.append(AsyncDBPool(cs.IS_SQL_CNX).connect())
            connections_tasks.append(AsyncDBPool(cs.WS_SQL_CNX).connect())
            connections_tasks.append(AsyncSOAP(cs.WS_SOAP_USER, cs.WS_SOAP_PASSWORD, cs.WS_SERVER_ID, cs.WS_SOAP_TIMEOUT, cs.WS_SOAP_URL).connect())
            connections_tasks.append(AsyncAMQP(cs.IS_AMQP_USER, cs.IS_AMQP_PASSWORD, cs.IS_AMQP_HOST, exchange_name='integration', exchange_type='topic').connect())
            self.__dbconnector_is, self.__dbconnector_ws, self.__soapconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
            await self.__amqpconnector.bind('entry_signals', ['status.*.entry', 'status.payment.finished', 'command.challenged.in', 'command.manual.open'], durable=True)
            pid = os.getpid()
            await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
            await self.__logger.info({'module': self.name, 'info': 'Started'})
            entry_records = await self.__dbconnector_ws
            return self
        except Exception as e:
            await self.__logger.exception({'module': self.name})
            raise e

    # blocking operation

    async def _get_photo(self, ip):
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(url=f'http://{ip}/axis-cgi/jpg/image.cgi?camera=1&resolution=1024x768&compression=25', timeout=2) as response:
                    result_raw = await response.content.read()
                    result = base64.b64encode(result_raw)
                    return result
        except:
            return None

    async def _get_plate_image(self, ip):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            try:
                async with session.get(url=f'http://{ip}/module.php?m=sekuplate&p=getImage&img=/home/root/tmp/last_read.jpg', timeout=2) as response:
                    result_raw = await response.content.read()
                    result = base64.b64encode(result_raw)
                    return result
            except:
                return None

    async def _get_plate_data(self, ip, ts):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            try:
                async with session.get(url=f'http://{ip}/module.php?m=sekuplate&p=letture', timeout=2) as response:
                    data = await response.content.read()
                    soup = BeautifulSoup(data, 'html.parser')
                    table_dict = {}
                    for row in soup.findAll('tr'):
                        aux = row.findAll('td')
                        table_dict[aux[0].string] = aux[1].string
                    result = {}
                    result.update({'confidence': float(table_dict['OCR SCORE'])*100})
                    result.update({'plate': table_dict['PLATE']})
                    result.update({'date': dp.parse(f"{table_dict['DATE']} {table_dict['HOUR'][0:2]}:{table_dict['HOUR'][3:5]}:{table_dict['HOUR'][6:8]}")})
                    if ts - result['date'].timestamp() <= 10:
                        return result
                    else:
                        return {'confidence': 0, 'plate': None, 'date': datetime.now()}
            except:
                return {'confidence': 0, 'plate': None, 'date': datetime.now()}

    async def _process_loop1_event(self, data, device):
        if data['value'] == 'OCCUPIED':
            photo1left = await self._get_photo(device['camPhoto1'])
            tasks = []
            tasks.append(self.__dbconnector_is.callproc('is_entry_loop1_ins', rows=0, values=[data['tra_uid'], data['act_uid'], data['device_id'], datetime.fromtimestamp(data['ts'])]))
            tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[data['tra_uid'], data['act_uid'],
                                                                                        photo1left, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
            tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.loop1.occupied'], priority=10))
            await asyncio.gather(*tasks)
        elif data['value'] == 'FREE':
            await asyncio.sleep(0.2)
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            if not temp_data is None:
                data['tra_uid'] = temp_data['transactionUID']
            await self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.loop2.free'], priority=10)

    async def _process_barrier_event(self, data, device):
        if data['value'] == 'OPENED':
            pre_tasks = []
            pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0]))
            pre_tasks.append(self.__dbconnector_ws.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
            pre_tasks.append(self._get_photo(device['camPhoto1']))
            pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
            pre_tasks.append(self._get_plate_image(device['camPlate']))
            temp_data, transit_data, photo2left, plate_data, plate_image = await asyncio.gather(*pre_tasks)
            if temp_data['transitionType'] != 'CHALLENGED':
                post_tasks = []
                post_tasks.append(self.__dbconnector_is.callproc('is_entry_barrier_ins', rows=0, values=[data['device_id'], data['act_uid'], transit_data.get('transitionType', None),
                                                                                                         json.dumps(transit_data, default=str), datetime.fromtimestamp(data['ts'])]))
                post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'],
                                                                                                 data['act_uid'], photo2left, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
                post_tasks.append(self.__dbconnector_is.callproc('is_plate_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], data['device_id'], plate_image,
                                                                                                 plate_data['confidence'], plate_data['plate'], plate_data['date']]))
                data['tra_uid'] = temp_data['transactionUID']
                post_tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
                await asyncio.gather(*post_tasks)
        elif data['value'] == 'CLOSED':
            await asyncio.sleep(0.2)
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            if not temp_data is None:
                data['tra_uid'] = temp_data['transactionUID']
            await self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.barrier.closed'], priority=10)
    # simulate as normal barrier event

    async def _process_command_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=['device_id']))
        pre_tasks.append(self._get_photo(device['camPhoto1']))
        pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
        pre_tasks.append(self._get_plate_image(device['camPlate']))
        temp_data, photo2left, plate_data, plate_image = await asyncio.gather(*pre_tasks)
        transit_data = {'transitionId': 0, 'transitionTS': datetime.now(), 'transitionArea': device['areaId'],
                        'transitionPlate': plate_data['plate'], 'transionStatus': 1, 'transitionTariff': -1,
                        'transitionTicket': '', 'subscriptionTicket': '', 'transitionType': 'CHALLENGED', 'transitionFine': 0}
        post_tasks = []
        post_tasks.append(self.__dbconnector_is.callproc('is_entry_command_ins', rows=0, values=[data['device_id'], data['act_uid'], json.dumps(transit_data, default=str), data['ts']]))
        post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'],
                                                                                         data['act_uid'], photo2left, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
        post_tasks.append(self.__dbconnector_is.callproc('is_plate_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], data['device_id'], plate_image,
                                                                                         plate_data['confidence'], plate_data['plate'], plate_data['date']]))
        if not temp_data is None:
            data['tra_uid'] = temp_data['transactionUID']
        post_tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
        await asyncio.gather(*post_tasks)

    async def _process_loop2_event(self, data, device):
        if data['value'] == 'OCCUPIED':
            pre_tasks = []
            pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0]))
            pre_tasks.append(self._get_photo(device['camPhoto2']))
            temp_data, photo3right = await asyncio.gather(*pre_tasks)
            post_tasks = []
            post_tasks.append(self.__dbconnector_is.callproc('is_entry_loop2_ins', rows=0, values=[data['device_id'], data['act_uid'], datetime.fromtimestamp(data['ts'])]))
            post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'],
                                                                                             data['act_uid'], photo3right, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
            if not temp_data is None:
                data['tra_uid'] = temp_data['transactionUID']
            post_tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.loop2.occupied'], priority=10))
            await asyncio.gather(*post_tasks)
        elif data['value'] == 'FREE':
            tasks = []
            # expect that loop2 was passed and session was closed
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            if not temp_data is None:
                if temp_data['transitioType'] != 'CHALLENGED':
                    data['tra_uid'] = temp_data['transactionUID']
                    event = self.OneShotEvent('OCCASIONAL_IN')
                    tasks.append(self.__amqpconnector.send(data=event.instance, persistent=True, keys=['event.occasional.in'], priority=10))
                    tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.loop2.free'], priority=10))
                elif temp_data['transitionType'] == 'CHALLENGED':
                    data['tra_uid'] = temp_data['transactionUID']
                    event = self.OneShotEvent('CHALLENGED_IN')
                    tasks.append(self.__amqpconnector.send(data=event.instance, persistent=True, keys=['event.challenged_in'], priority=10))
                    tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.challenged.in'], priority=10))
            tasks.append(self.__dbconnector_is.callproc('is_entry_confirm_upd', rows=0, values=[data['device_id'], datetime.fromtimestamp(data['ts'])]))
            await asyncio.sleep(0.2)
            await asyncio.gather(*tasks)

    async def _process_reverse_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_ws.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id']]))
        await asyncio.sleep(0.2)
        transit_data, temp_data = await asyncio.gather(*pre_tasks, return_exceptions=True)
        post_tasks = []
        if not temp_data is None:
            data['tra_uid'] = temp_data['transactionUID']
        post_tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.loop1.reverse'], priority=10))
        post_tasks.append(self.__dbconnector_is.callproc('is_entry_reverse_ins', rows=0, values=[data['device_id'], data['act_uid'],
                                                                                                 json.dumps(transit_data, default=str),  datetime.fromtimestamp(data['ts'])]))
        await asyncio.gather(*post_tasks)

    async def _process_lostticket_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_ws.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        pre_tasks.append(self.__dbconnector_ws.callproc('wp_payment_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        transit_data, payment_data = await asyncio.gather(*pre_tasks)
        post_tasks = []
        if not payment_data is None and int(payment_data['payPaid']) == 2500:
            if not transit_data is None:
                self.__dbconnector_is.callproc('is_entry_lost_ins', rows=0, values=[data['tra_uid'], data['device_id'], json.dumps(transit_data, default=str), datetime.fromtimestamp(data['ts'])])
        await asyncio.sleep(0.2)
        temp_data = self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 1])
        if not temp_data is None:
            data['tra_uid'] = temp_data['transactionUID']
        post_tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
        await asyncio.gather(*post_tasks)

    async def _process(self, redelivered, key, data):
        try:
            # check message keys
            # casual parking entry
            device = await self.__dbconnector_is.callproc('is_column_get', rows=1, values=[data['device_id']])
            if key == 'status.loop1.entry':
                await self._process_loop1_event(data, device)
            elif key == 'status.loop2.entry':
                await self._process_loop2_event(data, device)
            elif key == 'status.barrier.entry':
                await self._process_barrier_event(data, device)
            elif key == 'command.challenged.in':
                await self._process_command_event(data, device)
            elif key == 'status.reverse.entry':
                await self._process_reverse_event(data, device)
            # possible lost ticket entry
            elif key == 'status.payment.finished':
                await self._process_lostticket_event(data, device)
        except Exception as e:
            await self.__logger.exception({'module': self.name})

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, datetime.now()])
            try:
                await self.__amqpconnector.receive(self._process)
            except asyncio.CancelledError:
                pass
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, datetime.now()])

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_ws.disconnect())
        closing_tasks.append(self.__amqpconnector.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        asyncio.ensure_future(self._signal_cleanup())
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close the forked process
        sys.exit(0)

    def run(self):
        # use own event loop
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # try-except statement
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass
