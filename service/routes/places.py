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
name = 'ws_data'


class DataRequest(BaseModel):
    parking_number: Optional[int] = None
    parking_area: int = 1
    client_free: Optional[int] = None
    client_busy: Optional[int] = None
    vip_client_free: Optional[int] = None
    vip_client_busy: Optional[int] = None
    sub_client_free: Optional[int] = None
    sub_client_busy: Optional[int] = None
    error: int = 0


class DataResponse(BaseModel):
    parking_area: int
    error: int = 0


@router.get('/api/integration/v1/places')
async def get_data():
    tasks = BackgroundTasks()
    data = await ws.DBCONNECTOR_IS.callproc('is_places_get', rows=-1, values=[None])
    inner_tasks = []
    areas = set([d['areaId'] for d in data])
    for area in areas:
        inner_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_active_tickets_get', rows=1, values=[area]))
    tickets = await asyncio.gather(*inner_tasks)
    data_out = []
    for area in areas:
        area_places = {'areaId': area,
                       'areaDescription': next(d1['areaDescription'] for d1 in data if d1['areaId'] == area),
                       'occasional': {
                           'totalPlaces': next(d2['totalPlaces'] for d2 in data if d2['areaId'] == area and d2['clientType'] == 1),
                           'occupiedPlaces': next(d3['occupiedPlaces']for d3 in data if d3['areaId'] == area and d3['clientType'] == 1),
                           'freePlaces': next(d4['occupiedPlaces']for d4 in data if d4['areaId'] == area and d4['clientType'] == 1),
                           'activeTickets': next((t1['activeTickets'] for t1 in tickets if t1['areaId'] == area and t1['clientType'] == 1), 0)
                       },
                       'challenged': {
                           'totalPlaces': next(d5['totalPlaces'] for d5 in data if d5['areaId'] == area and d5['clientType'] == 2),
                           'occupiedPlaces': next(d6['occupiedPlaces']for d6 in data if d6['areaId'] == area and d6['clientType'] == 2),
                           'freePlaces': next(d7['occupiedPlaces']for d7 in data if d7['areaId'] == area and d7['clientType'] == 2),
                           'activeTickets': 0
                       },
                       'subscription': {
                           'totalPlaces': next((d8['totalPlaces'] for d8 in data if d8['areaId'] == area and d8['clientType'] == 3), 0),
                           'occupiedPlaces': next((d9['occupiedPlaces']for d9 in data if d9['areaId'] == area and d9['clientType'] == 3), 0),
                           'freePlaces': next((d10['occupiedPlaces']for d10 in data if d10['areaId'] == area and d10['clientType'] == 3), 0),
                           'activeTickets': next((t3['activeTickets'] for t3 in tickets if t3['areaId'] == area and t3['clientType'] == 3), 0)
                       }}
        data_out.append(area_places)
    return Response(json.dumps(data_out, default=str), status_code=200, media_type='application/json', background=tasks)


@router.post('/api/integration/v1/places', response_model=DataResponse)
async def upd_data(*, data: DataRequest):
    tasks = BackgroundTasks()
    uid = uuid4()
    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": 'Changedata', "request": data.dict(exclude_unset=True)})
    tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                             json.dumps({'uid': str(uid),  'request': data.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
    exec_tasks = []
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[0, 'Command', 'data']))
    exec_tasks.append(ws.DBCONNECTOR_IS.callproc('is_data_upd', rows=0, values=[data.client_busy, data.vip_client_busy, data.sub_client_busy, data.parking_area]))
    exec_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_data_upd', rows=0, values=[data.client_busy, data.sub_client_busy, data.parking_area]))
    try:
        await asyncio.gather(*exec_tasks)
        data.error = 0
        return Response(json.dumps(data.dict(exclude_unset=True)), status_code=200, media_type='application/json', background=tasks)
    except (ProgrammingError, OperationalError) as e:
        tasks.add_task(ws.LOGGER.error, {'module': name, 'error': repr(e)})
        data.error = 1
        return Response(json.dumps(data.dict(exclude_unset=True)), status_code=500, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.LOGGER.error, {'module': name, 'error': repr(e)})
        data.error = 1
        return Response(json.dumps(data.dict(exclude_unset=True)), status_code=403, media_type='application/json', background=tasks)
