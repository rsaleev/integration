
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
from utils.asyncamqp import AsyncAMQP
from utils.asyncsoap import AsyncSOAP
import configuration as cfg
import asyncio
import datetime
import json
import cv2
import base64
import functools
import uuid
import signal
from contextlib import suppress
import os


class EntryListener:
    def __init__(self, device_l: list):
        self.__dbconnector_wp: object = None
        self.__soapconector_wp: object = None
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
        await self.__logger.info({'module': self.name, 'info': f'Integration RDBS Connection: {self.__dbconnector_wp.connected}'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark RDBS Pool Connection'})
        self.__dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
        await self.__logger.info({'module': self.name, 'info': f'Wisepark RDBS Connection: {self.__dbconnector_wp.connected}'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing Wisepark SOAP Connection'})
        self.__soapconector_wp = await AsyncSOAP(cfg.soap_user, cfg.soap_password, cfg.object_id, cfg.soap_timeout, cfg.soap_url).connect()
        await self.__logger.info({'module': self.name, 'info': f'Wisepark SOAP Connection: {self.__soapconnector_wp.connected}'})
        await self.__logger.info({'module': self.name, 'info': 'Establishing AMQP Connection'})
        self.__amqpconnector = await AsyncAMQP(user=cfg.amqp_user, password=cfg.amqp_password, host=cfg.amqp_host, exchange_name='integration', exchange_type='topic').connect()
        # listen only events after loop2 was used
        await self.__amqpconnector.bind('statuses', ['status.entry.loop1', 'status.entry.loop2', 'status.entry.barrier'])
        await self.__logger.info({'module': self.name, 'info': f'AMQP Connection: {self.__amqpconnector.connected}'})
        return self

    def _get_photo(self, ip):
        cap = cv2.VideoCapture(f'rtsp://{ip}/axis-media/media.amp')
        if cap.isOpened():
            retval, image = cap.read()
            retval, buffer = cv2.imencode('.jpg', image)
            data = base64.b64encode(buffer)
            cap.release()
            return data

    async def _capture_plate(self, ter_id):
        self.__soapconector_wp.device = 0
        data = await self.__soapconnector_wp.client.service.GetPlate(sHeader=self.__soapconnector_wp.header,
                                                                     wTerId=ter_id)
        return data

   # callback
    async def _process(self, redelivered, key, data):
        if not redelivered:
            device = await self.__dbconnector_is.callproc('is_column_get', row=1, value=[data['device_address']])
            # 1st message loop 1 status
            if data['codename'] == 'BarrierLoop1Status' and data['value'] == 1:
                if not device['camPhoto1'] is None:
                    images = []
                    images.append(self._capture_plate(device['terId']))
                    images.append(self._capture_photo(device['camPhoto1']))
                    plate, photo = asyncio.gather(*images)
                    await self.__dbconnector_is.callproc('is_exit_ins', rows=0, values=[data['tra_uid'], data['device_address'], plate, photo, data['act_uid'], None, None, datetime.now()])
                else:
                    plate = await self._capture_plate(device['terId'])
                    await self.__dbconnector_is.callproc('is_exit_ins', rows=0, values=[data['tra_uid'], data['device_address'], plate, None, data['act_uid'], None, None, datetime.now()])
            # 2nd message barrier opened
            elif data['codename'] == 'BarrierStatus' and data['value'] == 1:
                transit_data = await self.__dbconnector_wp.сallproc('wp_exit_get', rows=1, values=[data['device_id']])
                await self.__dbconnector_is.callproc('is_exit_ins', rows=0, values=[data['tra_uid'], data['device_address'], None, None, None, data['act_uid'], transit_data, datetime.now()])
            # 2nd possible message loop 1 reversed car
            elif data['codename'] == 'Loop1Reverse':
                temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                if not temp_data is None and data['ts'].timestamp() - temp_data['ts'] <= 500:
                    await self.__dbconnector_is.callproc('is_exit_del', rows=0, values=[temp_data['transactionUID']])
            # 3rd message loop 2 status
            elif data['codename'] == 'BarrierLoop2Status' and data['value'] == 1:
                photo = None
                # check if camera photo #2 is bound to column
                if not device['camPhoto2'] is None:
                    photo = await self._capture_photo(data['camPhoto2'])
                temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                if data['ts'].timestamp() - temp_data['ts'] <= 500:
                    msg = {'transacation_uid': data['tra_uid'],
                           'plate_img': temp_data['plateImage'],
                           'plate_act_uid': temp_data['loop1ActUID'],
                           'photo_l': temp_data['rPhotoImage'],
                           'photo_l_act_uid': temp_data['loop1ActUID'],
                           'photo_r': photo,
                           'photo_r_act_uid': data['act_uid'],
                           'data': transit_data}
                    await self.__amqpconnector.send(data=msg, persistent=True, keys=['entry'], priority=1)
                else:
                    temp_data = await self.__dbconnector_is.callproc('is_exit_get', rows=1, values=[data['device_address']])
                    if not transit_data is None and data['ts'].timestamp() - temp_data['ts'] <= 500:
                        msg = {'transacation_uid': data['tra_uid'],
                               'plate_img': temp_data['plateImage'],
                               'plate_act_uid': temp_data['loop1ActUID'],
                               'photo_l': temp_data['rPhotoImage'],
                               'photo_l_act_uid': temp_data['loop1ActUID'],
                               'photo_r': None,
                               'photo_r_act_uid': None,
                               'data': transit_data}
                        await self.__amqpconnector.send(data=msg, persistent=True, keys=['entry'], priority=1)

    # dispatcher
    async def _dispatch(self):
        while True:
            await self.__amqpconnector.cbreceive(self._process)

    # use policy for own event loop
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        self.eventloop = asyncio.get_event_loop()
        # define signals
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(self._handler(s, self.eventloop)))
        # try-except statement for signals
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
            self.eventloop.run_forever()
        except:
            self.eventloop.close()
            os._exit(0)
