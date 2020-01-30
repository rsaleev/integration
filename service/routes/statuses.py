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


router = APIRouter()

name = 'webservice_statuses'


@router.get('/rest/monitoring/statuses')
async def get_devices_status():
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[None, None])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/statuses', 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)


@router.get('/rest/monitoring/statuses/{device_id}')
async def get_device_status(device_id):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[device_id, None])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': f'rest/monitoring/statuses/{device_id}', 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)


@router.get('/rest/monitoring/statuses/{device_id}/{stcodename}')
async def get_device_stcodename(device_id, stcodename):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[device_id, stcodename])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': f'rest/monitoring/statuses{device_id}/{stcodename}', 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)


@router.get('/rest/monitoring/statuses/{stcodename}')
async def get_device_stcodename(stcodename):
    tasks = BackgroundTasks()
    # try:
    data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[None, stcodename])
    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    # except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
    #     tasks.add_task(ws.logger.error, {'module': name, 'path': f'rest/monitoring/statuses/{stcodename}', 'error': repr(e)})
    #     return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)
