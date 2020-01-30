from fastapi.routing import APIRouter
import configuration as cfg
from starlette.responses import Response
from pydantic import BaseModel, validator, ValidationError
import json
import re
import dateutil.parser as dp
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError, InternalError
from pystemd.systemd1 import Unit
from pystemd.exceptions import PystemdRunError
from pystemd.dbusexc import DBusFileNotFoundError


router = APIRouter()


@router.get('/rest/monitoring/services')
async def get_services():
    try:
        services = await ws.dbconnector_is.callproc('is_services_get', rows=-1, values=[None, None])
        for s in services:
            service_processes = await ws.dbconnector_is.callproc(s['serviceName']+'_processes_get', rows=-1, calues=[None, None, None, None, None])
            s['serviceProcesses'] = service_processes
        return Response(json.dumps(services, default=str, ensure_ascii=False), status_code=200, media_type='application/json')
    except(OperationalError, ProgrammingError) as e:
        code, description = e.args
        if e == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')


@router.get('/rest/monitoring/services/{service_name}')
async def get_service(service_name):
    try:
        service = await ws.dbconnector_is.callproc('is_services_get', rows=1, values=[service_name, None])
        processes = await ws.dbconnector_is.callproc(f"{service_name}_processes_get", rows=-1, values=[service_name, None, None, None, None])
        service['serviceProcesses'] = processes
        return Response(json.dumps(service, default=str), status_code=200, media_type='application/json')
    except (OperationalError, ProgrammingError, InternalError) as e:
        code, description = e.args
        if code == 1146:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')
        elif code == 1305:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')


@router.get('/rest/monitoring/services/{service_name}/{operation}')
async def upd_services(service_name, operation):
    try:
        unit = Unit(f"{service_name}.service".encode())
        unit.load()
        if operation == 'stop':
            unit.Unit.Stop(b'replace')
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, default=str), status_code=200, media_type='application/json')
        elif operation == 'start':
            unit.Unit.Start(f"{service_name}.service".encode())
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, default=str), status_code=200, media_type='application/json')
        elif operation == 'restart':
            unit.Unit.Stop(b'replace')
            unit.Unit.Start(b'replace')
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, default=str), status_code=200, media_type='application/json')
        elif operation == 'status':
            pid = unit.Service.MainPID
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, default=str), status_code=200, media_type='application/json')
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': f"{operation} is not supported"}), status_code=403, media_type='application/json')
    except DBusFileNotFoundError:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=403, media_type='application/json')
