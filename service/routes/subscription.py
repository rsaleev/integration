from fastapi.routing import APIRouter
import configuration as cfg
from starlette.responses import Response
from pydantic import BaseModel, validator, ValidationError
from typing import Optional
import json
import re
import dateutil.parser as dp
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError
from pystemd.systemd1 import Unit
from pystemd.exceptions import PystemdRunError
from pystemd.dbusexc import DBusFileNotFoundError, DBusAccessDeniedError, DBusFailedError, DBusFileNotFoundError
from datetime import datetime


router = APIRouter()


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


@router.get('/rest/monitoring/subscription')
async def get_subscriptions():
    try:
        data = await cfg.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        await cfg.logger.error({'module': 'webservice', 'path': 'rest/monitoring/subscription', 'error': repr((e))})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500)


@router.get('/rest/monitoring/subscription/plate/{value}')
async def get_subscription_by_plate(value):
    try:
        data = await cfg.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, value, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        await cfg.logger.error({'module': 'webservice', 'path': 'rest/monitoring/subscription/plate', 'error': repr((e))})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500)


@router.get('/rest/monitoring/subscription/name/{value}')
async def get_subscription_by_name(value):
    try:
        data = await cfg.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, value, None, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        await cfg.logger.error({'module': 'webservice', 'path': 'rest/monitoring/subscription/name', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500)


@router.get('/rest/monitoring/subscription/cardcode/{data}')
async def get_subscription_by_cardcode(value):
    try:
        data = await cfg.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, value, None])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
    except Exception as e:
        await cfg.logger.error({'module': 'webservice', 'path': 'rest/monitoring/subscription/cardcode', 'error': repr(e)})
        return Response(json.dumsp({'error': 'INTERNAL_ERROR'}), default=str, media_type='application/json', status_code=500)

# 
@router.get('/rest/monitoring/subscription/cardid/{data}')
async def get_subscription_by_cardid(value):
    try:
        data = await cfg.dbconnector_wp.callproc('sub_get', rows=-1, values=[None, None, None, data])
        return Response(json.dumps(data), default=str, media_type='application/json', status_code=200)
     except(OperationalError, ProgrammingError) as e:
        await cfg.logger.error({'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')

# insert new record 
@router.put('/rest/monitoring/subscription')
async def add_subcription(subscription: Subscription)
   try:
        await cfg.dbconnector_wp.callproc('sub_ins', rows=0, values=[subscription.card_uid, subscription.car_plate, subscription.invalid_pass, subscription.sub_from, subscription.sub_to, subscription.card_num,
                                                                     subscription.sub_name, subscription.sub_email, subscriprion.sub_phone])
         return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
        await cfg.logger.error({'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
       

# update subscription data
@router.post('/rest/monitoring/subscription/{subid}')
async def add_subcription(subid, subscription: Subscription)
   try:
        await cfg.dbconnector_wp.callproc('sub_upd', rows=0, values=[subid, subscription.card_uid, subscription.car_plate, subscription.invalid_pass, subscription.sub_from, subscription.sub_to, subscription.card_num,
                                                                     subscription.sub_name, subscription.sub_email, subscriprion.sub_phone])
        return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
            code, description = e.args
            await cfg.logger.error({'module': 'webservice', 'error': repr(e)})
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')

# delete subscription data. Data will not be deleted from table but values will be changed to use this record again through update
@router.delete('/rest/monitoring/subscription/{subid}')
async def del_subscription(subid)
   try:
        await cfg.dbconnector_wp.callproc('sub_upd', rows=0, values=[subid, None, 'rezerved','rezerved', None, None, 'rezerved',
                                                                     'rezerved', 'rezerved', 'rezerved'])
        return Response(status_code=200)
    except(OperationalError, ProgrammingError) as e:
        await cfg.logger.error({'module': 'webservice', 'error': repr(e)})
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')        
