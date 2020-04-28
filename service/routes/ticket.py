from fastapi.routing import APIRouter
from starlette.responses import Response
import json
import re
import dateutil.parser as dp
from starlette.background import BackgroundTask, BackgroundTasks
import configuration as ws
from service import settings as ws
from aiomysql import OperationalError, InternalError, ProgrammingError
from pydantic import BaseModel


router = APIRouter()
name = 'webservice_subscrition'


class TicketRequest(BaseModel):
    ticket: str
    operation: str
    operator: str


@router.get('/rest/monitoring/ticket/{number}')
async def get_ticket(number):
    tasks = BackgroundTasks()
    tasks.add_task(ws.logger.info, {'module': name, 'path': f'rest/monitoring/ticket/number/{number}'})
    if re.match("^[1-9]{4}[0-9]{7}$", number) or re.match("[1-9]{4}[0-9]{20}$", number):
        try:
            if len(number) == 11:
                data = await ws.dbconnector_wp.callproc('wp_ticket_get', rows=1, values=[None, number])
                if not data is None:
                    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
                else:
                    return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=403, media_type='application/json')
            elif len(number) == 24:
                data = await ws.dbconnector_wp.callproc('wp_ticket_get', rows=-1, values=[number, None])
                if not data is None:
                    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
                else:
                    return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=403, media_type='application/json')
        except(OperationalError, ProgrammingError) as e:
            tasks.add_task(ws.logger.error, {'module': name, 'path': f"rest/monitoring/ticket/number/{number}", 'error': repr(e)})
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
    else:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')


@router.post('/rest/monitoring/ticket')
async def upd_ticket(req: TicketRequest):
    tasks = BackgroundTasks()
    if re.match("^[1-9]{4}[0-9]{7}$", req.ticket) or re.match("[1-9]{4}[0-9]{20}$", req.ticket):
        try:
            if req.operation == 'enable':
                if len(req.ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, req.ticket, 1, 0, req.operator, req.operation])
                    return Response('OK', status_code=200, media_type='application/json', background=tasks)
                elif len(req.ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[req.ticket, None, 1, 0, req.operator, req.operation])
                    return Response('OK', status_code=200, media_type='application/json', background=tasks)
            elif req.operation == 'free':
                if len(req.ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, req.ticket, 1, 0, req.operator, req.operation])
                    return Response('OK', status_code=200, media_type='application/json', background=tasks)
                elif len(req.ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[req.ticket, None, 1, 1, req.operator, req.operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            elif req.operation == 'disable':
                if len(req.ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, req.ticket, 0, 0, req.operator, req.operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(req.ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[req.ticket, None, 0, 0, req.operator, req.operation])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            else:
                return Response('FORBIDDEN', status_code=403, media_type='application/json')
        except:
            return Response('INTERNAL ERROR', status_code=500, media_type='application/json')
    else:
        return Response('WRONG_REQUEST', status_code=403, media_type='application/json')
