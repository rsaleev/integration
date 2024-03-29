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
import dateutil.parser as dp

router = APIRouter()

name = 'webservice_report'


@router.get('/monitoring/{tbl}')
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


@router.get('/report/grz')
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


@router.get('/report/consumables')
async def get_consumables(from_dt: str = None, to_dt=None):
    from_day = None
    from_year = None
    from_month = None
    to_day = None
    to_year = None
    to_month = None
    if from_dt:
        from_dt_dt = dp.parse(from_dt)
        from_year = from_dt_dt.year
        from_month = from_dt_dt.month
        from_day = from_dt_dt.day
    if to_dt:
        to_dt_dt = dp.parse(to_dt)
        to_year = to_dt_dt.year
        to_month = to_dt_dt.month
        to_day = to_dt_dt.day
    data = await ws.DBCONNECTOR_WS.callproc('rep_consumables', rows=1, values=[from_month, from_year, from_day, to_month, to_year, to_day])
    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
