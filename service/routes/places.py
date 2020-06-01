from fastapi.routing import APIRouter
from starlette.responses import Response
import json
from pydantic import BaseModel, ValidationError, validator
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration.settings as cs
from datetime import datetime
from typing import List, Optional
import integration.service.settings as ws
from starlette.background import BackgroundTasks
from uuid import uuid4
import asyncio
from itertools import groupby
from operator import itemgetter

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


@router.get('/api/integration/v1/places')
async def get_places():
    tasks = BackgroundTasks()
    try:
        places = await ws.DBCONNECTOR_IS.callproc('is_places_get', rows=-1, values=[None])
        areas = [p['areaId'] for p in places]
        tasks = []
        for area in areas:
            tasks.append(ws.DBCONNECTOR_WS.callproc('wp_active_tickets', rows=1, values=[area]))
        tickets = await asyncio.gather(*tasks)
        data = ([{"areaId": key,
                  "areaDescription": next(d1['areaDescription'] for d1 in places if d1['areaId'] == key),
                  "areaFloor":next(d2['areaFloor'] for d2 in places if d2['areaId'] == key),
                  "clientType":next(d3['client_type'] for d3 in places if d3['areaId'] == key),
                  "totalPlaces":next(d4['totalPlaces'] for d4 in places if d4['areaId'] == key),
                  "occupiedPlaces":next(d5['occupiedPlaces'] for d5 in places if d5['areaId'] == key),
                  "freePlaces":next(d6['freePlaces'] for d6 in places if d6['areaId'] == key),
                  "unavailablePlaces":next(d7['unavailablePlaces'] for d7 in places if d7['areaId'] == key),
                  "reservedPlaces":next(d8['reservedPlaces'] for d8 in places if d8['areaId'] == key),
                  "activeCommercial":next(d9['activeCommercial'] for d9 in tickets if d9['areaId'] == key),
                  "activeSubscription":next(d10['activeCommercial'] for d10 in tickets if d10['areaId'] == key),
                  "ts":next(d11['ts'] for d11 in places if d11['areaId'] == key)}
                 for key, group in groupby(data, key=lambda x: x['areaId'])])
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD REQUEST', 'comment': 'Not found'}), status_code=400, media_type='application/json', background=tasks)


@router.post('/api/integration/v1/places', response_model=PlacesResponse)
async def upd_places(*, places: PlacesRequest):
    tasks = BackgroundTasks()
    uid = uuid4()
    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": 'ChangePlaces', "request": places.dict(exclude_unset=True)})
    tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                             json.dumps({'uid': str(uid),  'request': places.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
    exec_tasks = []
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[0, 'Command', 'PLACES']))
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_places_upd', rows=0, values=[places.client_busy, places.vip_client_busy, places.sub_client_busy, places.parking_area]))
    exec_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_places_upd', rows=0, values=[places.client_busy, places.sub_client_busy, places.parking_area]))
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
        return Response(json.dumps(places.dict(exclude_unset=True)), status_code=403, media_type='application/json', background=tasks)
