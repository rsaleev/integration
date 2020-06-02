from fastapi.routing import APIRouter
from fastapi import Header
from starlette.responses import Response
import json
import re
import dateutil.parser as dp
from starlette.background import BackgroundTask, BackgroundTasks
import configuration as ws
import integration.service.settings as ws
from aiomysql import OperationalError, InternalError, ProgrammingError
from pydantic import BaseModel
import asyncio


router = APIRouter()
name = 'webservice_subscrition'


@router.get('/api/integration/v1/ticket/{number}')
async def get_ticket(number: str):
    tasks = BackgroundTasks()
    tasks.add_task(ws.LOGGER.info, {'module': name, 'path': f'rest/monitoring/ticket/number/{number}'})
    if len(number) == 11:
        data_wp = await ws.DBCONNECTOR_WS.callproc('wp_ticket_get', rows=1, values=[number, None])
        if not data_wp is None:
            tasks = []
            tasks.append(ws.DBCONNECTOR_IS.callproc('epp_tmpsession_get', rows=1, values=[data_wp['tidTraKey'], None, None, None, None, None]))
            tasks.append(ws.DBCONNECTOR_IS.callproc('epp_session_get', rows=1, values=[data_wp['tidTraKey'], None, None, None, None]))
            epp_tmp_session, epp_session = await asyncio.gather(*tasks)
            if not epp_session is None and epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': epp_session}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif epp_session is None and not epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': epp_tmp_session}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif epp_session is None and epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': None}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=403, media_type='application/json')
    elif len(number) == 24:
        data_wp = await ws.DBCONNECTOR_WS.callproc('wp_ticket_get', rows=-1, values=[number, None])
        if not data_wp is None:
            tasks = []
            tasks.append(ws.DBCONNECTOR_IS.callproc('epp_tmpsession_get', rows=1, values=[data_wp['tidTraKey'], None, None]))
            tasks.append(ws.DBCONNECTOR_IS.callproc('epp_session_get', rows=1, values=[data_wp['tidTraKey'], None, None, None, None]))
            epp_tmp_session, epp_session = await asyncio.gather(*tasks)
            if not epp_session is None and epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': epp_session}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif epp_session is None and not epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': epp_tmp_session}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif epp_session is None and epp_tmp_session is None:
                data = {'wiseparkData': data_wp, 'eppData': None}
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Ticket not found'}), status_code=403, media_type='application/json')
    else:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Wrong ticket format. Expected format: ([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')


@router.get('/api/integration/v1/ticket/{number}/{operation}')
async def upd_ticket(number, operation, operator: str = Header('UserName', convert_underscores=False)):
    tasks = BackgroundTasks()
    if re.match("^[1-9]{4}[0-9]{7}$", number) or re.match("[1-9]{4}[0-9]{20}$", number):
        try:
            if operation == 'enable':
                if len(number) == 11:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[None, number, 1, 0, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(number) == 24:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[number, None, 1, 0, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            elif operation == 'free':
                if len(number) == 11:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[None, number, 1, 0, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(number) == 24:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[number, None, 1, 1, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            elif operation == 'disable':
                if len(number) == 11:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[None, number, 0, 0, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(number) == 24:
                    tasks.add_task(ws.DBCONNECTOR_WS.callproc, 'wp_ticket_upd', rows=0, values=[number, None, 0, 0, operator, operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            else:
                return Response('FORBIDDEN', status_code=403, media_type='application/json')
        except:
            return Response('INTERNAL ERROR', status_code=500, media_type='application/json')
    else:
        return Response('WRONG_REQUEST', status_code=403, media_type='application/json')
