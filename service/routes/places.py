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
from uuid import uuid4
import asyncio

router = APIRouter()
name = 'ws_places'


class PlacesRequest(BaseModel):
    parking_number: Optional[int] = None
    parking_area: int = 1
    client_free: Optional[int] = None
    client_busy: Optional[int] = None
    vip_client_free: Optional[int] = None
    vip_client_busy: Optional[int] = None
    sub_client_free: Optional[int] = None
    sub_client_busy: Optional[int] = None
    error: int = 0


class PlacesResponse(BaseModel):
    parking_area: int
    error: int = 0


@router.get('/api/integration/v1/monitoring/places')
async def get_places():
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_places_get', rows=-1, values=[None])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD REQUEST', 'comment': 'Not found'}), status_code=400, media_type='application/json', background=tasks)


@router.post('/api/integration/v1/monitoring/places', response_model=PlacesResponse)
async def upd_places(*, places: PlacesRequest):
    tasks = BackgroundTasks()
    uid = uuid4()
    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": 'ChangePlaces', "request": places.dict(exclude_unset=True)})
    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                             json.dumps({'uid': str(uid),  'request': places.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
    exec_tasks = []
    exec_tasks.append(ws.dbconnector_is.callproc('is_status_upd', rows=0, values=[0, 'Command', 'PLACES']))
    exec_tasks.append(ws.dbconnector_is.callproc('is_places_upd', rows=0, values=[places.client_busy, places.vip_client_busy, places.sub_client_busy, places.parking_area]))
    exec_tasks.append(ws.dbconnector_wp.callproc('wp_places_upd', rows=0, values=[places.client_busy, places.sub_client_busy, places.parking_area]))
    try:
        await asyncio.gather(*exec_tasks)
        places.error = 0
        return Response(json.dumps(places.dict(exclude_unset=True)), status_code=200, media_type='application/json', background=tasks)
    except (ProgrammingError, OperationalError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        places.error = 1
        return Response(json.dumps(places.dict(exclude_unset=True)), status_code=500, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        places.error = 1
        return Response(json.dumps(places.dict(exclude_unset=True)), status_code=400, media_type='application/json', background=tasks)
