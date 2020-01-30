from fastapi.routing import APIRouter
from starlette.responses import Response
import json
import re
import dateutil.parser as dp
from starlette.background import BackgroundTask, BackgroundTasks
import configuration as ws
from service import settings as ws


router = APIRouter()
name = 'webservice_subscrition'


@router.get('/rest/monitoring/ticket/number/{ticket}')
async def get_ticket(ticket):
    tasks = BackgroundTasks()
    tasks.add_task(ws.logger.info, {'module': name, 'path': f'rest/monitoring/ticket/number/{ticket}'})
    if re.match("^[1-9]{4}[0-9]{7}$", ticket) or re.match("[1-9]{4}[0-9]{20}$", ticket):
        try:
            if len(ticket) == 11:
                data = await ws.dbconnector_wp.callproc('wp_ticket_get', rows=-1, values=[None, ticket])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif len(ticket) == 24:
                data = await ws.dbconnector_wp.callproc('wp_ticket_get', rows=-1, values=[ticket, None])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        except(OperationalError, ProgrammingError) as e:
            tasks.add_task(ws.logger.error, {'module': name, 'path': f"rest/monitoring/ticket/number/{ticket}", 'error': repr(e)})
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
    else:
        return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')


@router.get('/rest/monitoring/ticket/{ticket}/{operation}')
async def upd_ticket(ticket, operation):
    tasks = BackgroundTasks()
    tasks.add_task(ws.logger.info, {'module': name, 'path': f'rest/monitoring/ticket/number/{ticket}/{operation}'})
    if re.match("[0-9]{11}", ticket) or re.match("[0-9]{24}"):
        try:
            if operation == 'reactivate':
                if len(ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, ticket, True, False])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[ticket, None, True, False])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            elif operation == 'free':
                if len(ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, ticket, True, True])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[ticket, None, True, True])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            elif operation == 'deactivate':
                 if len(ticket) == 11:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[None, ticket, False, False])
                    return Response(status_code=200, media_type='application/json', background=tasks)
                elif len(ticket) == 24:
                    tasks.add_task(ws.dbconnector_wp.callproc, 'wp_ticket_upd', rows=0, values=[ticket, None, False, False])
                    return Response(status_code=200, media_type='application/json', background=tasks)
            else:
                return Response(json.dumps({'error': 'FORBIDDEN', 'comment': f"Forbidden"}), status_code=403, media_type='application/json', background=task)
        except(OperationalError, ProgrammingError) as e:
            tasks.add_task(ws.logger.error, {'module': name, 'path': f'rest/monitoring/ticket/number/{ticket}/{operation}', 'error': repr(e)})
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json', background=tasks)
            else:
                return Response(json.dumps({'error': 'INTERNAL_ERROR', 'comment': 'Internal error'}), status_code=404, media_type='application/json', background=tasks)
    else:
        return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')
