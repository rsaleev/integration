from fastapi.routing import APIRouter
import configuration as cfg
from service import settings as ws
from starlette.responses import Response
from starlette.background import BackgroundTasks
from pydantic import BaseModel, validator, ValidationError
from typing import Optional
import json
import re
import dateutil.parser as dp
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError
from datetime import datetime


router = APIRouter()
name = 'webservice_subscription'


class Subscription(BaseModel):
    sub_id: Optional[int]
    operation: Optional[str]
    car_plate: str
    card_uid: str
    card_num: str
    sub_from: datetime
    sub_to: datetime
    sub_name: str
    sub_email: Optional[str]
    sub_phone: Optional[str]
    invalid_pass: Optional[str]


def converter(code):
    # 4 bytes Mifare
    if len(code) == 8:
        b1 = code[0:2]
        b2 = code[2:4]
        b3 = code[4:6]
        b4 = code[6:8]
        pre_str = b4+b3+b2+b1
        came_code = str(int(pre_str, 16))
        if len(came_code) < 10:
            came_code = '00000000080' + came_code
        else:
            came_code = '0000000008' + came_code
        return came_code
    # 7 bytes
    elif len(code) == 14:
        byte_1 = str(int('8'+code[0:6], 16))
        if len(byte_1) == 9:
            byte_1_9 = byte_1[8:10]
            byte_1 = byte_1[0:8] + '0'+byte_1_9
        byte_2 = str(int(code[7:14], 16))
        if len(byte_2) == 8:
            byte_2_0 = byte_2[0:2]
            byte_2 = '0'+byte_2_0+byte_2[2:8]
        came_code = '0'+str(byte_1)+str(byte_2)
        return came_code


@router.get('/rest/monitoring/subscription')
async def get_subscriptions():
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/subscription', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500, background=tasks)


@router.get('/rest/monitoring/subscription/plate/{value}')
async def get_subscription_by_plate(value):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, value, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/subscription/plate', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500, background=tasks)


@router.get('/rest/monitoring/subscription/name/{value}')
async def get_subscription_by_name(value):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, value, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/subscription/name', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500, background=tasks)


@router.get('/rest/monitoring/subscription/cardcode/{data}')
async def get_subscription_by_cardcode(value):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, value, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/subscription/cardcode', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500, background=tasks)

#
@router.get('/rest/monitoring/subscription/cardid/{data}')
async def get_subscription_by_cardid(value):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, None, data])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except (OperationalError, ProgrammingError) as e:
        code, description = e.args
        tasks = BackgroundTasks()
        tasks.add_task(ws.logger.error, {'module': name, 'path': f'rest/monitoring/subscription/cardid/{value}', 'error': repr(e)})
        tasks.add_task(ws.logger.error, {'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        else:
            return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal Error'}), status_code=404, media_type='application/json', background=tasks)


# insert new record
@router.put('/rest/monitoring/subscription')
async def add_subcription(subscription: Subscription):
    try:
        await ws.dbconnector_wp.callproc('sub_ins', rows=0, values=[subscription.card_uid, subscription.car_plate, subscription.invalid_pass, subscription.sub_from, subscription.sub_to, subscription.card_num,
                                                                    subscription.sub_name, subscription.sub_email, subscription.sub_phone])
        return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
        tasks = BackgroundTasks()
        tasks.add_task(ws.logger.error, {'module': 'webservice', 'error': repr(e)})
        code, description = e.args
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)


# update subscription data
@router.post('/rest/monitoring/subscription/{subid}')
async def add_subcription(subid, subscription: Subscription):
    try:
        await ws.dbconnector_wp.callproc('sub_upd', rows=0, values=[subid, subscription.card_uid, subscription.car_plate, subscription.invalid_pass, subscription.sub_from, subscription.sub_to, subscription.card_num,
                                                                    subscription.sub_name, subscription.sub_email, subscription.sub_phone])
        return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
        code, description = e.args
        tasks = BackgroundTasks()
        tasks.add_task(ws.logger.error, {'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)

# delete subscription data. Data will not be deleted from table but values will be changed to use this record again through update
@router.delete('/rest/monitoring/subscription/{subid}')
async def del_subscription(subid):
    try:
        await ws.dbconnector_wp.callproc('sub_upd', rows=0, values=[subid, None, 'rezerved', 'rezerved', None, None, 'rezerved',
                                                                    'rezerved', 'rezerved', 'rezerved'])
        return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
        code, description = e.args
        tasks = BackgroundTasks()
        tasks.add_task(ws.logger.error, {'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
