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


class dataRequest(BaseModel):
    parking_number: Optional[int] = None
    parking_area: int = 1
    client_free: Optional[int] = None
    client_busy: Optional[int] = None
    vip_client_free: Optional[int] = None
    vip_client_busy: Optional[int] = None
    sub_client_free: Optional[int] = None
    sub_client_busy: Optional[int] = None
    error: int = 0


class dataResponse(BaseModel):
    parking_area: int
    error: int = 0


@router.get('/api/integration/v1/places')
async def get_data():
    tasks = BackgroundTasks()
    data = await ws.DBCONNECTOR_IS.callproc('is_places_get', rows=-1, values=[None])
    inner_tasks = []
    for area in [d['areaId'] for d in data]:
        inner_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_active_tickets_get', rows=1, values=[area]))
    tickets = await asyncio.gather(*inner_tasks)
    for d in data:
        d['activeTickets'] = next((t['activeTickets'] for t in tickets if t['clientType'] == d['clientType'] and t['areaId'] == d['areaId']), 0)
    data_out = ([{"areaId": key,
                  "terDescription": next(d1['terDescription'] for d1 in data if d1['areaId'] == key),
                  "areaPlaces": [({'date': g['ts'],
                                   'clientType':'occasional' if g['clientType'] == 1 else 'challenged' if g['clientType'] == 2 else 'subscription',
                                   'totalPlaces':g['totalPlaces'],
                                   'occupiedPlaces':g['occupiedPlaces'],
                                   'freePlaces':g['freePlaces'],
                                   'unavailablePlaces':g['unavailablePlaces'],
                                   'reserveredPlaces':g['reservedPlaces'],
                                   'activeTickets':g['activeTickets']}) for g in group]}
                 for key, group in groupby(data, key=lambda x: x['areaId'])])
    return Response(json.dumps(data_out, default=str), status_code=200, media_type='application/json', background=tasks)


@router.post('/api/integration/v1/places', response_model=dataResponse)
async def upd_data(*, data: dataRequest):
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
