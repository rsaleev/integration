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


class PlacesRequest(BaseModel):
    parking_number: Optional[int]
    parking_area: int = 1
    client_free: Optional[int]
    client_busy: int
    vip_client_free: Optional[int]
    vip_client_busy: int
    error: int = 1


class PlacesResponse(BaseModel):
    error: int


@router.get('/rest/monitoring/places')
async def get_places():
    tasks = BackgroundTasks()
    try:
        data = await ws.dbconnector_is.callproc('is_places_get', rows=-1, values=[None])
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)


@router.post('/rest/monitoring/places')
async def upd_places(*, places: Places, response_model=PlacesResponse):
    tasks = BackgroundTasks()
    try:
        await ws.dbconnector_is.callproc('is_places_upd', rows=0, values=[places.client_busy, places.vip_client_busy, places.sub_client_busy, places.parking_area])
        await ws.dbconnector_wp.callproc('wp_places_upd', rows=0, values=[places.client_free, places.parking_area])
        return Response(json.dumps(PlacesResponse.dict()), status_code=200)
    except (ProgrammingError, OperationalError) as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        places.error = 0
        return Response(json.dumps(PlacesResponse.dict()), status_code=500, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        places.error = 0
        return Response(json.dumps(PlacesResponse.dict()), status_code=403, media_type='application/json', background=tasks)
