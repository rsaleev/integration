
import asyncio
from datetime import datetime
import json
import cv2
import base64
import functools
import signal
import os
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from utils.asyncsoap import AsyncSOAP
import configuration as cfg
from datetime import timedelta


class ExitListener:
    def __init__(self):
        self.__dbconnector_wp: object = None
        self.__soapconnector_wp: object = None
        self.__amqpconnector: object = None
        self.__logger: object = None
        self.__eventloop: object = None
        self.__eventsignal: bool = False
        self.name = 'Exitsistener'

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
        await self.__amqpconnector.bind('exit_signals', ['status.*.exit'], durable=True)
        pid = os.getpid()
        await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, pid])
        await self.__logger.info({'module': self.name, 'info': 'Started'})
        return self

    async def _capture_plate(self, ter_id):
        try:
            data = await self.__soapconnector_wp.execute('GetPlate', header=True, device=ter_id,  wTerId=ter_id)
            return data
        except:
            data['rConfidence'] = 0
            data['rImage'] = None
            return data
        finally:
            await asyncio.sleep(0.2)

    # blocking operation
    def _get_photo(self, ip):
        cap = cv2.VideoCapture(f'rtsp://{ip}/axis-media/media.amp')
        if cap.isOpened():
            retval, image = cap.read()
            retval, buffer = cv2.imencode('.jpg', image)
            data = base64.b64encode(buffer)
            cap.release()
            return data

    # asyncio implementation
    async def _capture_photo(self, ip):
        data = await self.eventloop.run_in_executor(None, functools.partial(self._get_photo, (ip)))
        return data

    async def _process(self, redelivered, key, data):
        try:
            if not redelivered:
                device = await self.__dbconnector_is.callproc('is_column_get', rows=1, values=[data['device_id']])
                if data['codename'] == 'BarrierLoop1Status' and data['value'] == 'OCCUPIED':
                    images = []
                    # get plate and photo from left camera
                    images.append(self._capture_plate(device['terId']))
                    if not device['camPhoto1'] is None and device['camPhoto1'] != '':
                        images.append(self._capture_photo(device['camPhoto1']))
                        # suppress exception
                    futures = await asyncio.gather(*images, return_exceptions=True)
                    # try-except to suppress unpack error
                    try:
                        plate, photo = futures
                        plate_accuracy = plate['rConfidence']
                        plate_img = plate['rImage'] if plate_accuracy > 0 else None
                        await self.__dbconnector_is.callproc('is_exit_ins', rows=0, values=[data['tra_uid'], data['device_address'], plate_img, plate_accuracy, photo, data['act_uid'], datetime.now()])
                    except:
                        plate, = futures
                        plate_accuracy = plate['rConfidence']
                        plate_img = plate['rImage'] if plate_accuracy > 0 else None

                        await self.__dbconnector_is.callproc('is_exit_ins', rows=0, values=[data['tra_uid'], data['device_address'], plate_img, plate_accuracy, None, data['act_uid'], datetime.now()])
                # 2nd message barrier opened
                elif data['codename'] == 'BarrierStatus' and data['value'] == 'OPENED':
                    transit_data = await self.__dbconnector_wp.callproc('wp_entry_get', rows=1, values=[data['device_id']])
                    # with barrier act uid
                    await self.__dbconnector_is.callproc('is_exit_barrier_ins', rows=0, values=[data['device_address'], data['act_uid'], transit_data['transitionUID'], json.dumps(transit_data, default=str), datetime.now()])
                # 2nd  possible message loop 1 reversed car
                elif data['codename'] == 'Loop1Reverse' and data['value'] == 'REVERSED':
                    # check if temp data is stored and delete record
                    temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                    if temp_data['transactionData'] is None:
                        await self.__dbconnector_is.callproc('is_entry_del', rows=0, values=[temp_data['transactionUID']])
                    else:
                        temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                        temp_data['ampp_id'] = data['ampp_id']
                        temp_data['ampp_type'] = data['ampp_type']
                        await self.__amqpconnector.send(data=temp_data, persistent=True, keys=['exit.reversed'], priority=1)
                # 3rd message loop 2 status
                elif data['codename'] == 'BarrierLoop2Status' and data['value'] == 'OCCUPIED':
                    # check if camera #2 is bound to column
                    photo = None
                    if not device['camPhoto2'] is None and device['camPhoto2'] != '':
                        photo = await self._capture_photo(device['camPhoto2'])
                    await self.__dbconnector_is.callproc('is_exit_loop2_ins', rows=0, values=[data['device_address'], data['act_uid'], photo, datetime.now()])
                    temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                    temp_data['ampp_id'] = data['ampp_id']
                    temp_data['ampp_type'] = data['ampp_type']
                    # get services for those entry data must be sent
                    await self.__amqpconnector.send(data=temp_data, persistent=True, keys=['exit.normal'], priority=1)
                await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1])
        except:
            raise

    # dispatcher
    async def _dispatch(self):
        while not self.eventsignal:
            await self.__amqpconnector.receive(self._process)
        else:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 0])
            await asyncio.sleep(0.5)

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
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())
