
from bs4 import BeautifulSoup
from dateutil import parser as dp
import asyncio
from datetime import datetime
import json
import base64
import functools
import uuid
import signal
import os
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from utils.asyncsoap import AsyncSOAP
import configuration as cfg
from datetime import timedelta
from uuid import uuid4
import aiohttp
import base64


class EntryListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__dbconnector_is: object = None
        self.__soapconnector_wp: object = None
        self.__amqpconnector: object = None
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

    async def _initialize(self):
        self.__logger = await AsyncLogger().getlogger(cfg.log)
        await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
        connections_tasks = []
        connections_tasks.append(AsyncDBPool(conn=cfg.is_cnx).connect())
        connections_tasks.append(AsyncDBPool(conn=cfg.wp_cnx).connect())
        connections_tasks.append(AsyncSOAP(login=cfg.soap_user, password=cfg.soap_password, parking_id=cfg.object_id, timeout=cfg.soap_timeout, url=cfg.soap_url).connect())
        connections_tasks.append(AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect())
        self.__dbconnector_is, self.__dbconnector_wp, self.__soapconnector_wp, self.__amqpconnector = await asyncio.gather(*connections_tasks)
        await self.__amqpconnector.bind('entry_signals', ['status.*.entry', 'status.payment.*', 'command.challenged.in'], durable=True)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid])
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self

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
        await self.__dbconnector_is.callproc('is_entry_ins', rows=0, values=[data['tra_uid'], data['act_uid'], data['device_id'], datetime.now()])
        photo1left = await self._get_photo(device['camPhoto1'])
        await self.__dbconnector_is.callprorc('is_photo_ins', rows=0, values=[data['tra_uid'], data['act_uid'], photo1left, data['device_id'], device['camPhoto1']])

    async def _process_barrier_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_temp_get', rows=1, values=['device_id']))
        pre_tasks.append(self.__dbconnector_wp.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        pre_tasks.append(self._get_photo(device['camPhoto1']))
        pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
        pre_tasks.append(self._get_plate_image(device['camPlate']))
        temp_data, transit_data, photo2left, plate_data, plate_image = await asyncio.gather(*pre_tasks)
        if temp_data['transitionType'] != 'CHALLENGED':
            post_tasks = []
            post_tasks.append(self.__dbconnector_is.callproc('is_entry_upd', rows=0, values=[data['device_id'], data['act_uid'], transit_data.get('transitionType', None),
                                                                                             json.dumps(transit_data, default=str), None, 0, data['ts']]))
            post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], photo2left, data['device_id'], device['camPhoto1']]))
            post_tasks.append(self.__dbconnector_is.callproc('is_plate_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], data['device_id'], plate_image,
                                                                                             plate_data['confidence'], plate_data['plate'], plate_data['date']]))
            await asyncio.gather(*post_tasks)

    async def _process_command_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_temp_get', rows=1, values=['device_id']))
        pre_tasks.append(self._get_photo(device['camPhoto1']))
        pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
        pre_tasks.append(self._get_plate_image(device['camPlate']))
        temp_data, photo2left, plate_data, plate_image = await asyncio.gather(*pre_tasks)
        transit_data = {'transitionId': 0, 'transitionTS': datetime.now(), 'transitionArea': device['areaId'],
                        'transitionPlate': plate_data['plate'], 'transionStatus': 1, 'transitionTariff': -1,
                        'transitionTicket': '', 'subscriptionTicket': '', 'transitionType': 'CHALLENGED', 'transitionFine': 0}
        post_tasks = []
        post_tasks.append(self.__dbconnector_is.callproc('is_entry_upd', rows=0, values=[data['device_id'], data['act_uid'], transit_data.get('transitionType', None),
                                                                                         json.dumps(transit_data, default=str), None, 0, data['ts']]))
        post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], photo2left, data['device_id'], device['camPhoto1']]))
        post_tasks.append(self.__dbconnector_is.callproc('is_plate_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], data['device_id'], plate_image,
                                                                                         plate_data['confidence'], plate_data['plate'], plate_data['date']]))
        await asyncio.gather(*post_tasks)

    async def _process_loop2_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_temp_get', rows=1, values=['device_id']))
        pre_tasks.append(self._get_photo(device['camPhoto2']))
        temp_data, photo3right = await asyncio.gather(*pre_tasks)
        post_tasks = []
        post_tasks.append(self.__dbconnector_is.callproc('is_entry_upd', rows=0, values=[data['device_id'], data['act_uid'], None,
                                                                                         None, data['act_uid'], 1, data['ts']]))
        post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], photo3right, data['device_id'], device['camPhoto1']]))
        await asyncio.gather(*post_tasks)

    async def _process_reverse_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_wp.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_partial_get', rows=1, values=[data['device_address']]))
        transit_data, temp_data = await asyncio.gather(*pre_tasks, return_exceptions=True)
        post_tasks = []
        data['tra_uid'] = temp_data['transactionUID']
        # simulate normal processing and send data to exchange instantly
        post_tasks.apppend(self.__amqpconnector.send(data=data, persistent=True, keys=['active.entry.reverse'], priority=10))
        post_tasks.append(self.__amqpconnector.send(data=temp_data, persistent=True, keys=['entry.reversed'], priority=8))
        # post processing operations
        post_tasks.append(self.__dbconnector_is.callproc('is_entry_upd', rows=0, values=[data['device_id'], data['act_uid'], 'REVERSED',
                                                                                         json.dumps(transit_data, default=str), None, 1, data['ts']]))
        await asyncio.gather(*post_tasks)

    async def _process_lostticket_event(self, data, device):
        transit_data = await self.__dbconnector_wp.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])])
        await self.__dbconnector_is.callproc('is_entry_ins', rows=0, values=[data['tra_uid'], data['act_uid'], data['device_id'], datetime.now()])
        await self.__dbconnector_is.callproc('is_entry_upd', rows=0, values=[data['device_id'], None, 'LOST',
                                                                             json.dumps(transit_data, default=str), None, 1, data['ts']])

    async def _process(self, redelivered, key, data):
        try:
            if not redelivered:
                # check message keys
                # casual parking entry
                tasks = []
                if key in ['status.loop1.entry', 'status.loop2.entry', 'status.barrier.entry', 'command.challenged.in']:
                    device = await self.__dbconnector_is.callproc('is_column_get', rows=1, values=[data['device_id']])
                    if data['codename'] == 'BarrierLoop1Status':
                        if data['value'] == 'OCCUPIED':
                            await self._process_loop1_event(data, device)
                        elif data['value'] == 'FREE':
                            temp_data = await self.__dbconnector_is.callproc('is_entry_temp_get', rows=1, values=[data['device_address']])
                            data['tra_uid'] = temp_data['transactionUID']
                            await self.__amqpconnector.send(data, persistent=True, keys=['active.entry.loop1'], priority=10)
                      # 2nd message barrier opened
                    elif data['codename'] == 'Command' and data['value'] == 'MANUALLY_OPEN':
                        await self._process_command_event(data, device)
                    elif data['codename'] == 'BarrierStatus' and data['value'] == 'OPENED':
                        await self._process_barrier_event(data, device)
                    elif data['codename'] == 'BarrierStatus' and data['value'] == 'CLOSED':
                        temp_data = await self.__dbconnector_is.callproc('is_entry_partial_get', rows=1, values=[data['device_address']])
                        data['tra_uid'] = temp_data['transactionUID']
                        await self.__amqpconnector.send(data=data, persistent=True, keys=['active.entry.barrier'], priority=10)
                    # 2nd  possible message loop 1 reversed car
                    elif data['codename'] == 'Loop1Reverse' and data['value'] == 'REVERSED':
                        # check if temp data is stored and delete record
                        await self._process_reverse_event(data, device)
                    # 3rd message loop 2 status
                    elif data['codename'] == 'BarrierLoop2Status' and data['value'] == 'OCCUPIED':
                        await self._process_loop2_event(data, device)
                    elif data['codename'] == 'BarrierLoop2Status' and data['value'] == 'FREE':
                        tasks = []
                        temp_data = await self.__dbconnector_is.callproc('is_entry_temp_get', rows=1, values=[data['device_address']])
                        data['tra_uid'] = temp_data['transactionUID']
                        tasks.append(self.__amqpconnector.send(data=data, persistent=True, keys=['active.entry.loop2'], priority=9))
                        tasks.append(self.__dbconnector_is.callproc('is_entry_confirm_upd', rows=0, values=[data['device_address']]))
                        await asyncio.gather(*tasks)
                # possible lost ticket entry
                elif key == 'status.payment.finished':
                    # try to fetch data from DB
                    print(data)
                    transit_data = await self.__dbconnector_wp.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])])
                    print(transit_data)
                    if not transit_data is None and transit_data['transactionType'] == 'LOST' and transit_data['transitionFine'] > 0:
                        tasks = []
                        # simulate
                        tasks.append(self.__dbconnector_is.callproc('is_entry_barrier_ins', rows=0, values=[
                            data['tra_uid'], data['device_address'], None, json.dumps(transit_data, default=str), datetime.now()]))
                    await asyncio.gather(*tasks)

        except:
            raise

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
            try:
                await self.__amqpconnector.receive(self._process)
            except (ChannelClosed, ChannelInvalidStateError):
                pass
            except asyncio.CancelledError:
                pass
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0])

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__amqpconnector.disconnect()
        await self.__logger.shutdown()

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
        # close process
        os._exit(0)

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
