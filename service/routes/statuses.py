from fastapi.routing import APIRouter
import configuration as cfg
import service.settings as ws
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.background import BackgroundTasks
from pydantic import BaseModel, validator, ValidationError
from typing import Optional
import json
import re
import dateutil.parser as dp
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError


router = APIRouter()
name = 'statuses'


@router.get('/rest/monitoring/statuses')
async def get_devices_status(request: Request):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[None, None])
        return JSONResponse(data, status_code=200)
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': request.url.path, 'params': request.path_params, 'error': repr(e)})
        return JSONResponse({'error': 'BAD_REQUEST'}, status_code=400, background=tasks)


@router.get('/rest/monitoring/statuses/{device_id}')
async def get_device_status(request: Request, device_id):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[device_id, None])
        return JSONResponse(data, status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': request.url.path, 'params': request.path_params, 'error': repr(e)})
        return JSONResponse({'error': 'BAD_REQUEST'}, status_code=400, background=tasks)


@router.get('/rest/monitoring/statuses/{device_id}/{stcodename}')
async def get_device_stcodename(request: Request, device_id, stcodename):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[device_id, stcodename])
        return JSONResponse(data, status_code=200)
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': request.url.path, 'params': request.path_params, 'error': repr(e)})
        return JSONResponse({'error': 'BAD_REQUEST'}, status_code=400, background=tasks)


@router.get('/rest/monitoring/statuses/{stcodename}')
async def get_device_stcodename(request: Request, stcodename):
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[None, stcodename])
        return JSONResponse(data, status_code=200)
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': request.url.path, 'params': request.path_params, 'error': repr(e)})
        return JSONResponse({'error': 'BAD_REQUEST'}, status_code=400, background=tasks)
