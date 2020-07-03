
import asyncio
import base64
import functools
import json
import os
import signal
import sys
import uuid
from datetime import date, datetime, timedelta
from uuid import uuid4

import aiohttp
import toml
import uvloop
from bs4 import BeautifulSoup
from dateutil import parser as dp
import setproctitle
from tenacity import (AsyncRetrying, RetryError, retry,
                      retry_if_exception_type, stop_after_attempt)

import configuration.settings as cs
from utils.asyncamqp import AsyncAMQP, ChannelClosed, ChannelInvalidStateError
from utils.asynclckhouse import AsyncClickHouse
from utils.asynclog import AsyncLogger
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool
from utils.tbparser import parse_tb


class EntryListener:
    def __init__(self):
        self.__dbconnector_ws: object = None
        self.__dbconnector_is: object = None
        self.__amqpconnector_is: object = None
        self.__clckhouseconnector_is: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.__clickhouseconnector_is: object = None
        self.name = 'EntryListener'
        self.source = 'integration'
        self.alias = 'is-entries'
        self.__ampp_id = None
        self.__server_ip = None

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
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, v):
        return self.__eventsignal

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    class OneShotEvent:
        def __init__(self, value, device_ip, ampp_id):
            self.__codename = 'OneShotEvent'
            self.__value: str = value
            self.__device_id = 0
            self.__device_address = 0
            self.__device_ip = cs.WS_SERVER_IP
            self.__device_type = 0
            self.__ampp_id = f'{ampp_id}'
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
        setproctitle.setproctitle(self.alias)
        configuration = toml.load(cs.CONFIG_FILE)
        self.__ampp_id = configuration['ampp']['id']
        self.__server_ip = configuration['wisepark']['server_ip']
        self.__dbconnector_is = AsyncDBPool(host=configuration['integration']['rdbs']['host'],
                                            port=configuration['integration']['rdbs']['port'],
                                            login=configuration['integration']['rdbs']['login'],
                                            password=configuration['integration']['rdbs']['password'],
                                            database=configuration['integration']['rdbs']['database'])
        # limit pool size to max = 5
        self.__dbconnector_ws = AsyncDBPool(host=configuration['wisepark']['rdbs']['host'],
                                            port=configuration['wisepark']['rdbs']['port'],
                                            login=configuration['wisepark']['rdbs']['login'],
                                            password=configuration['wisepark']['rdbs']['password'],
                                            database=configuration['wisepark']['rdbs']['database'],
                                            min_size=1, max_size=5)
        self.__amqpconnector_is = AsyncAMQP(login=configuration['integration']['amqp']['login'],
                                            password=configuration['integration']['amqp']['password'],
                                            host=configuration['integration']['amqp']['host'],
                                            exchange_name=configuration['integration']['amqp']['exchange'],
                                            exchange_type=configuration['integration']['amqp']['exchange_type'])
        self.__clickhouseconnector_is = AsyncClickHouse(url=configuration['integration']['clickhouse']['url'],
                                                       login=configuration['integration']['clickhouse']['login'],
                                                       password=configuration['integration']['clickhouse']['password'],
                                                       database=configuration['integration']['clickhouse']['integration'])
        # initialize logger
        self.__logger = AsyncLogger(f'{cs.LOG_PATH}/integratiob.log')
        await self.__logger.getlogger()
        await self.__logger.info({'module': self.name, 'info': 'Statrting...'})
        # initialize connectors
        connections_tasks = []
        connections_tasks.append(self.__dbconnector_is.connect())
        connections_tasks.append(self.__dbconnector_ws.connect())
        connections_tasks.append(self.__amqpconnector_is.connect())
        try:
            await asyncio.gather(*connections_tasks)
            await self.__amqpconnector_is.bind('entry_signals', ['status.*.entry', 'status.payment.finished', 'command.challenged.in', 'command.manual.open'], durable=True)
            await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
            await self.__logger.info({'module': self.name, 'info': 'Started'})
            return self
        except:
            await self.__logger.exception({'module': self.name})

    async def _get_photo(self, ip):
        log_tasks = []
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(url=f'http://{ip}/axis-cgi/jpg/image.cgi?camera=1&resolution=1024x768&compression=25', timeout=2) as response:
                    result_raw = await response.content.read()
                    result = base64.b64encode(result_raw)
                    log_tasks.append(self.__clickhouseconnector_is.execute('INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES',
                                                                          (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps({"fetched_bytes": len(result)}), 'axis.data')))
                    log_tasks.append(self.__logger.debug({'module': self.name, 'fetched_img_bytes': len(result) if result else 0}))
                    return result, log_tasks
        except Exception as e:
            log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                                  (date.today(), datetime.now(), self.source, self.name, 'exception', json.dumps(parse_tb(e)), 'traceback.data')))
            log_tasks.append(self.__logger.exception({'module': self.name}))
            return None, log_tasks

    async def _get_plate_image(self, ip):
        log_tasks = []
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            try:
                async with session.get(url=f'http://{ip}/module.php?m=sekuplate&p=getImage&img=/home/root/tmp/last_read.jpg', timeout=2) as response:
                    result_raw = await response.content.read()
                    result = None
                    if result_raw:
                        result = base64.b64encode(result_raw)
                    # debugging logs
                    log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                                          (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps({'image': True if result else False}), 'traceback.data')))
                    log_tasks.append(self.__logger.debug({'module': self.name, 'fetched_img_bytes': len(result) if result else 0}))
                    return result, log_tasks
            except Exception as e:
                log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                                      (date.today(), datetime.now(), self.source, self.name, 'exception', json.dumps(parse_tb(e)), 'traceback.data')))
                log_tasks.append(self.__logger.exception({'module': self.name}))
                return None, log_tasks

    async def _get_plate_data(self, ip, ts):
        log_tasks = []
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
                    # default unsuccessfull output 
                    result_out = {'confidence': 0, 'plate': None, 'date': datetime.now()}
                    if ts - result['date'].timestamp() <= 10:
                        result_out = result
                    log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                     (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(result), 'bs4_fetched.data')))
                    log_tasks.append(self.__logger.debug({'module': self.name, 'plate_data': result_out}))
            except Exception as e:
                log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                                      (date.today(), datetime.now(), self.source, self.name, 'exception', json.dumps(parse_tb(e)), 'traceback.data')))
                log_tasks.append(self.__logger.exception({'module': self.name}))
                return {'confidence': 0, 'plate': None, 'date': datetime.now()}, log_tasks

    async def _process_loop1_event(self, data, device):
        log_tasks = []
        log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                              (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(data), 'amqp.data')))
        log_tasks.append(self.__logger.debug({'module': self.name, 'amqp_incoming': json.dumps(data)}))
        if data['value'] == 'OCCUPIED':
            photo_data = await self._get_photo(device['camPhoto1'])
            photo1left, photo_log_tasks = photo_data
            log_tasks.extend(photo_log_tasks)
            tasks = []
            tasks.append(self.__dbconnector_is.callproc('is_entry_loop1_ins', rows=0, values=[data['tra_uid'], data['act_uid'], data['device_id'], datetime.fromtimestamp(data['ts'])]))
            tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[data['tra_uid'], data['act_uid'],
                                                                                        photo1left, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
            tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.loop1.occupied'], priority=10))
            await asyncio.gather(*tasks)
        elif data['value'] == 'FREE':
            await asyncio.sleep(0.2)
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                                   (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(temp_data), 'sql.data')))
            log_tasks.append(self.__logger.debug({'module': self.name, 'temp_data_extracted': json.dumps(temp_data)}))
            await self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.loop2.free'], priority=10)
        await asyncio.gather(*log_tasks, return_exceptions=True)

    async def _process_barrier_event(self, data, device):
        log_tasks = []
        log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                              (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(data), 'amqp.data')))
        log_tasks.append(self.__logger.debug({'module': self.name, 'amqp_incoming': json.dumps(data)}))
        if data['value'] == 'OPENED':
            pre_tasks = []
            pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0]))
            pre_tasks.append(self.__dbconnector_ws.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
            pre_tasks.append(self._get_photo(device['camPhoto1']))
            pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
            pre_tasks.append(self._get_plate_image(device['camPlate']))
            temp_data, transit_data, photo2left, plate_data_out, plate_image_out = await asyncio.gather(*pre_tasks)
            log_tasks.append(self.__logger.debug({'module': self.name, 'sql_fetched': json.dumps(temp_data)}))
            log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                      (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(temp_data), 'sql.data')))
            # fetched BS4 data
            plate_data, plate_data_log_tasks = plate_data_out
            # fetched image
            plate_image, plate_image_log_tasks = plate_image_out 
            log_tasks.extend(plate_data_log_tasks)
            log_tasks.extend(plate_image_log_tasks)
            if temp_data['transitionType'] != 'CHALLENGED':
                post_tasks = []
                post_tasks.append(self.__dbconnector_is.callproc('is_entry_barrier_ins', rows=0, values=[data['device_id'], data['act_uid'], transit_data.get('transitionType', None),
                                                                                                         json.dumps(transit_data, default=str), datetime.fromtimestamp(data['ts'])]))
                post_tasks.append(self.__dbconnector_is.callproc('is_photo_ins', rows=0, values=[temp_data['transactionUID'],
                                                                                                 data['act_uid'], photo2left, data['device_id'], device['camPhoto1'], datetime.fromtimestamp(data['ts'])]))
                post_tasks.append(self.__dbconnector_is.callproc('is_plate_ins', rows=0, values=[temp_data['transactionUID'], data['act_uid'], data['device_id'], plate_image,
                                                                                                 plate_data['confidence'], plate_data['plate'], plate_data['date']]))
                data['tra_uid'] = temp_data['transactionUID']
                post_tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
                await asyncio.gather(*post_tasks)
        elif data['value'] == 'CLOSED':
            log_tasks.append(self.__logger.debug({'module': self.name, 'amqp_incoming': json.dumps(data)}))
            log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel,logData, logDataType) VALUES",
                                                                 (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(data), 'amqp.data')))
            await asyncio.sleep(0.2)
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            log_tasks.append(self.__logger.debug({'module': self.name, 'sql_retrieved': json.dumps(temp_data)}))
            log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel,logData, logDataType) VALUES",
                                                     (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(temp_data), 'sql.data')))
            if not temp_data is None:
                data['tra_uid'] = temp_data['transactionUID']
            await self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.barrier.closed'], priority=10)
        await asyncio.gather(*log_tasks, return_exceptions=True)
    # simulate as normal barrier event

    async def _process_command_event(self, data, device):
        log_tasks = []
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=['device_id']))
        pre_tasks.append(self._get_photo(device['camPhoto1']))
        pre_tasks.append(self._get_plate_data(device['camPlate'], data['ts']))
        pre_tasks.append(self._get_plate_image(device['camPlate']))
        temp_data, photo_data, plate_data_out, plate_image_out = await asyncio.gather(*pre_tasks)
        photo2left, photo_log_tasks = photo_data
        plate_data, plate_data_log_tasks = plate_data_out
        plate_image, plate_image_log_tasks = plate_image_out
        log_tasks.extend(photo_log_tasks)
        log_tasks.extend(plate_data_log_tasks)
        log_tasks.extend(plate_image_log_tasks)
        log_tasks.append(self.__logger.debug({'module': self.name, 'sql_retrieved': json.dumps(temp_data)}))
        log_tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel,logData, logDataType) VALUES",
                                           (date.today(), datetime.now(), self.source, self.name, 'debug', json.dumps(temp_data), 'sql.data')))
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
        post_tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
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
            data['tra_uid'] = temp_data['transactionUID']
            post_tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.loop2.occupied'], priority=10))
            await asyncio.gather(*post_tasks)
        elif data['value'] == 'FREE':
            tasks = []
            # expect that loop2 was passed and session was closed
            temp_data = await self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id'], 0])
            if temp_data['transitioType'] != 'CHALLENGED':
                data['tra_uid'] = temp_data['transactionUID']
                event = self.OneShotEvent('OCCASIONAL_IN', data['device_ip'], data['ampp_id'])
                tasks.append(self.__amqpconnector_is.send(data=event.instance, persistent=True, keys=['event.occasional.in'], priority=10))
                tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.loop2.free'], priority=10))
            elif temp_data['transitionType'] == 'CHALLENGED':
                data['tra_uid'] = temp_data['transactionUID']
                event = self.OneShotEvent('CHALLENGED_IN', data['device_ip'], data['ampp_id'])
                tasks.append(self.__amqpconnector_is.send(data=event.instance, persistent=True, keys=['event.challenged_in'], priority=10))
                tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.challenged.in'], priority=10))
            tasks.append(self.__dbconnector_is.callproc('is_entry_confirm_upd', rows=0, values=[data['device_id'], datetime.fromtimestamp(data['ts'])]))
            await asyncio.gather(*tasks)

    async def _process_reverse_event(self, data, device):
        pre_tasks = []
        pre_tasks.append(self.__dbconnector_ws.callproc('wp_entry_get', rows=1, values=[data['device_id'], int(data['ts'])]))
        pre_tasks.append(self.__dbconnector_is.callproc('is_entry_get', rows=1, values=[data['device_id']]))
        await asyncio.sleep(0.2)
        transit_data, temp_data = await asyncio.gather(*pre_tasks)
        post_tasks = []
        data['tra_uid'] = temp_data['transactionUID']
        post_tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.loop1.reverse'], priority=10))
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
        data['tra_uid'] = temp_data['transactionUID']
        post_tasks.append(self.__amqpconnector_is.send(data=data, persistent=True, keys=['event.entry.barrier.opened'], priority=10))
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
            tasks = []
            tasks.append(self.__logger.exception({'module': self.name}))
            # clickhouse logging
            tasks.append(self.__clickhouseconnector_is.execute("INSERT INTO integration.integration_logs (logDate, logDateTime, logSource, logSourceModule, logLevel, logData, logDataType) VALUES",
                                                              (date.today(), datetime.now(), self.source, self.name, 'exception', json.dumps(parse_tb(e)), 'traceback')
                                                              ))
            await asyncio.gather(*tasks, return_exceptions=True)

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, datetime.now()])
            try:
                await self.__amqpconnector_is.receive(self._process)
            except asyncio.CancelledError:
                pass
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0, datetime.now()])

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_ws.disconnect())
        closing_tasks.append(self.__amqpconnector_is.disconnect())
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
        uvloop.install()
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
