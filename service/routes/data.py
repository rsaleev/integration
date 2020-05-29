from fastapi.routing import APIRouter
from starlette.responses import Response
from starlette.background import BackgroundTasks
import json
import re
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
import integration.service.settings as ws
import asyncio
from datetime import date, datetime, timedelta
from itertools import groupby
from operator import itemgetter


router = APIRouter()

name = 'webservice_report'


@router.get('/api/integration/v1/monitoring/{tbl}')
async def get_view(tbl: str):
    tasks = BackgroundTasks()
    try:
        data = await ws.DBCONNECTOR_WS.callproc('wp_table_get', rows=-1, values=[tbl])
        return Response(json.dumps({'pageData': data}, default=str, ensure_ascii=False), status_code=200, media_type='application/json')
    except (OperationalError, ProgrammingError) as e:
        tasks.add_task(ws.LOGGER.info, {'module': name, 'path': f"rest/monitoring/data/{tbl}", 'error': repr(e)})
        code, description = e.args
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Forbidden'}), status_code=403, media_type='application/json', background=tasks)


@router.get('/api/integration/v1/report/grz')
async def get_grz(ter_id: int = None, from_dt: str = None, to_dt: str = None):
    data = await ws.DBCONNECTOR_IS.callproc('rep_plates_get', rows=-1, values=[ter_id, from_dt, to_dt])
    data = sorted(data, key=lambda x: x['terType'])
    data_out = ([{"terAddress": key,
                  "terDescription": next(d1['terDescription'] for d1 in data if d1['terAddress'] == key),
                  "camMode":next(d2['camMode'] for d2 in data if d2['terAddress'] == key),
                  "camPlateData": [({'date': g['repDate'],
                                     'totalTransits':g['totalTransits'],
                                     'more6symbols':g['more6symbols'],
                                     'less6symbols':g['less6symbols'],
                                     'noSymbols':g['noSymbols'],
                                     'accuracy':g['accuracy']}) for g in group]}
                 for key, group in groupby(data, key=lambda x: x['terAddress'])])
    return Response(json.dumps(data_out, default=str), status_code=200, media_type='application/json')
