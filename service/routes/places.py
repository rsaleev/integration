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
    try:
        data = await ws.DBCONNECTOR_IS.callproc('is_places_get', rows=-1, values=[None])
        areas = [p['areaId'] for p in data]
        data_tasks = []
        for area in areas:
            data_tasks.append(ws.DBCONNECTOR_WS.callproc('wp_active_tickets', rows=1, values=[area]))
        tickets = await asyncio.gather(*data_tasks)
        data.append(tickets)
        data_out = ([{"areaId": key,
                      "areaDescription": next(d1['areaDescription'] for d1 in data if d1['areaId'] == key),
                      "areaFloor":next(d2['areaFloor'] for d2 in data if d2['areaId'] == key),
                      "clientType":next(d3['client_type'] for d3 in data if d3['areaId'] == key),
                      "totaldata":next(d4['totaldata'] for d4 in data if d4['areaId'] == key),
                      "occupieddata":next(d5['occupieddata'] for d5 in data if d5['areaId'] == key),
                      "freedata":next(d6['freedata'] for d6 in data if d6['areaId'] == key),
                      "unavailabledata":next(d7['unavailabledata'] for d7 in data if d7['areaId'] == key),
                      "reserveddata":next(d8['reserveddata'] for d8 in data if d8['areaId'] == key),
                      "activeCommercial":next(d9['activeCommercial'] for d9 in tickets if d9['areaId'] == key),
                      "activeSubscription":next(d10['activeCommercial'] for d10 in tickets if d10['areaId'] == key),
                      "ts":next(d11['ts'] for d11 in data if d11['areaId'] == key)}
                     for key, group in groupby(data, key=lambda x: x['areaId'])])
        return data_out
    except Exception as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        return Response(json.dumps({'error': 'BAD REQUEST', 'comment': repr(e)}), status_code=400, media_type='application/json', background=tasks)


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
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        data.error = 1
        return Response(json.dumps(data.dict(exclude_unset=True)), status_code=500, media_type='application/json', background=tasks)
    except ValidationError as e:
        tasks.add_task(ws.logger.error, {'module': name, 'error': repr(e)})
        data.error = 1
        return Response(json.dumps(data.dict(exclude_unset=True)), status_code=403, media_type='application/json', background=tasks)
