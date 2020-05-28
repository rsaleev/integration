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
from datetime import datetime, date

router = APIRouter()

name = 'webservice_logs'


@router.get('/api/integration/v1/monitoring/logs/{source}')
async def get_devices_status(source, rows: int = 10, level: str = None, from_date: str = None, to_date: str = None):
    tasks = BackgroundTasks()
    if level in ['debug', 'error', 'critical', 'warning', 'info'] or level is None:
        date_from = date.today() if from_date is None else from_date
        date_to = date.today() if to_date is None else to_date
        try:
            data = await ws.DBCONNECTOR_IS.callproc('is_logs_get', rows=-1, values=[source, level, date_from, to_date, rows])
            return Response(json.dumps(data, default=str, indent=4, skipkeys=True), status_code=200, media_type='application/json')
        except Exception as e:
            tasks.add_task(ws.LOGGER.error, {'module': name, 'path': 'rest/monitoring/statuses', 'error': repr(e)})
            return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': repr(e)}, default=str), status_code=500, media_type='application/json', background=tasks)
    else:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Wrong parameter'}, default=str), status_code=500, media_type='application/json', background=tasks)
