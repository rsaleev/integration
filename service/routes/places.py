from fastapi.routing import APIRouter
from starlette.responses import Response
import json
from pydantic import BaseModel, ValidationError, validator
import re
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
from datetime import datetime
from dateutil import parser as dp
from typing import List
from aiomysql import OperationalError, DatabaseError, ProgrammingError
import service.settings as ws
from aiomysql import OperationalError, ProgrammingError
from starlette.background import BackgroundTasks


router = APIRouter()

name = 'ws_places'


class Places(BaseModel):
    type: str = 'places'
    parking_number: int
    client_free: int
    client_busy: int
    vip_client_free: int
    vip_client_busy: int
    date_event: str
    error: int = 0


@router.get('/rest/v1/monitoring/places')
async def get_places():
    data = await ws.dbconnector_is.callproc('is_places_get', rows=-1, values=[None])
    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')


@router.post('/rest/v1/monitoring/places')
async def upd_places(*, places: List[Places]):
    tasks = BackgroundTasks()
    try:
        for p in places:
            try:
                await ws.dbconnector_wp.callproc('wp_places_upd', rows=0, values=[p.parking_number, p.client_free])
                await ws.dbconnector_is.callproc('is_places_upd', rows=0, values=[p.client_busy, p.vip_client_busy, p.parking_number])
                return Response(status_code=200)
            except (ProgrammingError, OperationalError) as e:
                tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
                return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Connection refused'}), status_code=404, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not valid request'}), status_code=403, media_type='application/json', background=tasks)


@router.get('/rest/monitoring/tabarea')
async def get_places():
    data = await ws.dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
    return Response(json.dumps({"pageData": data}, default=str), status_code=200, media_type='application/json')


@router.post('/rest/monitoring/places')
async def upd_places(*, places: List[Places]):
    tasks = BackgroundTasks()
    try:
        for p in places:
            try:
                await ws.dbconnector_wp.callproc('wp_places_upd', rows=0, values=[p.parking_number, p.client_free])
                await ws.dbconnector_is.callproc('is_places_upd', rows=0, values=[p.client_busy, p.vip_client_busy, p.parking_number])
                return Response(status_code=200)
            except (ProgrammingError, OperationalError) as e:
                tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
                return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Connection refused'}), status_code=404, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not valid request'}), status_code=403, media_type='application/json', background=tasks)
