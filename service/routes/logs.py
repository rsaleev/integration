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


class LogRequest(BaseModel):
    rows: Optional[int]
    level: Optional[int]
    from_dt: Optional[int]
    to_dt: Optional[int]


@route('/rest/monitoring/logs/{source}')
async def get_devices_status(source, log_request: LogRequest):
    tasks = BackgroundTasks()
    try:
        if not log_request.rows is None and log_request.rows > 0:
            data = await ws.dbconnector_is.callproc('is_logs_get', rows=log_request.rows, values=[source, log_request.level, log_request.from_dt, log_request.to_dt])
            return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        else:
            data = await ws.dbconnector_is.callproc('is_logs_get', rows=-1, values=[source, log_request.level, log_request.from_dt, log_request.to_dt])
            return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/statuses', 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)


@route('/rest/monitoring/logs/')
async def get_devices_status(source, log_request: LogRequest):
    tasks = BackgroundTasks()
    try:
        if not log_request.rows is None and log_request.rows > 0:
            data = await ws.dbconnector_is.callproc('is_logs_get', rows=log_request.rows, values=[None, log_request.level, log_request.from_dt, log_request.to_dt])
            return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        else:
            data = await ws.dbconnector_is.callproc('is_logs_get', rows=-1, values=[source, log_request.level, log_request.from_dt, log_request.to_dt])
            return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/statuses', 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}, default=str), status_code=500, media_type='application/json', background=tasks)
