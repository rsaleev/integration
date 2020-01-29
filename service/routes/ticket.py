from fastapi.routing import APIRouter
from starlette.responses import Response
import json
import re
import dateutil.parser as dp
from starlette.background import BackgroundTask
import configuration as cfg


router = APIRouter()


@router.get('/rest/monitoring/ticket/number/{ticket}')
async def get_ticket(ticket):
    if re.match("^[1-9]{4}[0-9]{7}$", ticket) or re.match("[1-9]{4}[0-9]{20}$", ticket):
        try:
            if len(ticket) == 11:
                data = await cfg.dbconnector_wp.callproc('rem_ticket_show', rows=-1, values=[None, ticket])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif len(ticket) == 24:
                data = await cfg.dbconnector_wp.callproc('rem_ticket_show', rows=-1, values=[ticket, None])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        except(OperationalError, ProgrammingError) as e:
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
    else:
        return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')


@router.get('/rest/monitoring/ticket/number/{ticket}')
async def get_ticket(ticket):
    if re.match("^[1-9]{4}[0-9]{7}$", ticket) or re.match("[1-9]{4}[0-9]{20}$", ticket):
        try:
            if len(ticket) == 11:
                data = await cfg.dbconnector_wp.callproc('wp_ticket_get', rows=-1, values=[None, ticket])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
            elif len(ticket) == 24:
                data = await cfg.dbconnector_wp.callproc('wp_ticket_get', rows=-1, values=[ticket, None])
                return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
        except(OperationalError, ProgrammingError) as e:
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
    else:
        return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')


@router.get('/rest/monitoring/ticket/{ticket}/{operation}')
async def upd_ticket(ticket, operation):
    if re.match("[0-9]{11}", ticket) or re.match("[0-9]{24}"):
        try:
            if operation == 'reactivate':
                if len(ticket) == 11:
                    task = BackgroundTask(cfg.dbconnector_wp.callproc, 'rem_ticket_reactivate', rows=0, values=[None, ticket])
                    return Response(status_code=200, media_type='application/json', background=task)
                elif len(ticket) == 24:
                    task = BackgroundTask(cfg.dbconnector_wp.callproc, 'rem_ticket_reactivate', rows=0, values=[ticket, None])
                    return Response(status_code=200, media_type='application/json', background=task)
            elif operation == 'free':
                if len(ticket) == 11:
                    task = BackgroundTask(cfg.dbconnector_wp.callproc, 'rem_ticket_free', rows=0, values=[None, ticket])
                    return Response(status_code=200, media_type='application/json', background=task)
                elif len(ticket) == 24:
                    task = BackgroundTask(cfg.dbconnector_wp.callproc, 'rem_ticket_free', rows=0, values=[ticket, None])
                    return Response(status_code=200, media_type='application/json', background=task)
            else:
                return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': f"Wrong request param {operation}"}), status_code=403, media_type='application/json', background=task)
       except(OperationalError, ProgrammingError) as e:
            code, description = e.args
            if code == 1146:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
            elif code == 1305:
                return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
    else:
        return Response(json.dumps({'error': 'WRONG_REQUEST', 'comment': 'Wrong ticket format. Expected format: str([0-9]{11} or [0-9]{24})'}), status_code=403, media_type='application/json')
