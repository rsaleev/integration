from fastapi.routing import APIRouter
from starlette.responses import Response
import json
from pydantic import BaseModel, ValidationError, validator
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
from datetime import datetime
from typing import List, Optional
import service.settings as ws
from starlette.background import BackgroundTasks

router = APIRouter()

name = 'ws_places'


class Places(BaseModel):
    type: str = 'places'
    parking_number: int
    client_free: int
    client_busy: Optional[int]
    vip_client_free: int
    vip_client_busy: Optional[int]
    date_event: str
    error: Optional[int] = 0


@router.get('/rest/monitoring/places')
async def get_places():
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_places_get', rows=-1, values=[None])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except (ProgrammingError, OperationalError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Connection refused'}), status_code=404, media_type='application/json', background=tasks)


@router.get('/rest/monitoring/tabarea')
async def get_places():
    data = await ws.dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
    return Response(json.dumps({"pageData": data}, default=str), status_code=200, media_type='application/json')


@router.post('/rest/monitoring/places')
async def upd_places(*, places: List[Places]):
    tasks = BackgroundTasks()
    try:
        for index, place in enumerate(places):
            await ws.dbconnector_is.callproc('is_places_upd', rows=0, values=[place.client_free, place.vip_client_free, index+1])
            await ws.dbconnector_wp.callproc('wp_places_upd', rows=0, values=[place.client_free, index+1])
        return Response(status_code=200)
    except (ProgrammingError, OperationalError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Connection refused'}), status_code=404, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not valid request'}), status_code=403, media_type='application/json', background=tasks)
