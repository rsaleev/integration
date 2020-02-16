from fastapi.routing import APIRouter
from fastapi import Query
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

name = 'webservice_logs'


@router.get('/rest/monitoring/logs/{source}')
async def get_devices_status(source: str, rows: int = 10, level: str = None, from_dt: str = None, to_dt: str = None):
    tasks = BackgroundTasks()
    if level in ['debug', 'error', 'critical', 'warning', 'info'] or level is None:
        try:
            data = await ws.dbconnector_is.callproc('is_logs_get', rows=-1, values=[source, level, from_dt, to_dt, rows])
        except (DatabaseError, DataError, OperationalError, ProgrammingError, InternalError, IntegrityError) as e:
            tasks.add_task(ws.logger.error, {'module': name, 'path': 'rest/monitoring/statuses', 'error': repr(e)})
            return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': repr(e)}, default=str), status_code=500, media_type='application/json', background=tasks)
        else:
            return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')

    else:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Wrong parameter'}, default=str), status_code=500, media_type='application/json', background=tasks)
