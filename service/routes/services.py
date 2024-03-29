import asyncio
import json
from typing import Optional

import dateutil.parser as dp
from aiomysql import (DatabaseError, DataError, InternalError,
                      OperationalError, ProgrammingError)
from fastapi.routing import APIRouter
from pydantic import BaseModel, ValidationError, validator
from pystemd.dbusexc import (DBusAccessDeniedError, DBusFailedError,
                             DBusFileNotFoundError)
from pystemd.exceptions import PystemdRunError
from pystemd.systemd1 import Unit
from starlette.background import BackgroundTasks
from starlette.responses import JSONResponse, Response

import integration.service.settings as ws

router = APIRouter()


class ServiceConfigRequest(BaseModel):
    enabled: int
    url: Optional[str]
    entry_notify: Optional[int]
    exit_notify: Optional[int]
    payment_notify: Optional[int]
    status_notify: Optional[int]
    money_notify: Optional[int]
    invenotry_notify: Optional[int]


@router.get('/api/integration/v1/services')
async def get_services():
    try:
        services = await ws.DBCONNECTOR_IS.callproc('is_services_get', rows=-1, values=[None, None, None, None, None, None, None, None])
        for s in services:
            try:
                s['processes'] = await ws.DBCONNECTOR_IS.callproc(f"{s['serviceName']}_processes_get", rows=-1, values=[None, None, None, None])
            except:
                s['processes'] = []
        data_out = {'modulesServices': services}
        return Response(json.dumps(data_out, default=str), status_code=200, media_type='application/json')
    except Exception as e:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')


@router.get('/api/integration/v1/service/{service_name}')
async def get_service(service_name):
    try:
        service = await ws.DBCONNECTOR_IS.callproc('is_services_get', rows=1, values=[service_name, None])
        processes = await ws.DBCONNECTOR_IS.callproc(f"{service_name}_processes_get", rows=-1, values=[None, None, None, None, None])
        service['serviceProcesses'] = processes
        return Response(json.dumps(service), status_code=200, media_type='application/json')
    except Exception as e:
        return Response(json.dumps({'error': 'BAD_REQUEST', 'comment': 'Not found'}), status_code=404, media_type='application/json')


@router.get('/api/integration/v1/service/{service_name}/{operation}')
async def control_services(service_name, operation):
    tasks = BackgroundTasks()
    try:
        unit = Unit(f"{service_name}.service".encode())
        unit.load()
        if operation == 'stop':
            unit.Unit.Stop(b'replace')
            return JSONResponse({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, status_code=200, media_type='application/json')
        elif operation == 'start':
            unit.Unit.Start(f"{service_name}.service".encode())
            return JSONResponse({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}, status_code=200, media_type='application/json')
        elif operation == 'restart':
            unit.Unit.Stop(b'replace')
            unit.Unit.Start(b'replace')
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}), status_code=200, media_type='application/json')
        elif operation == 'status':
            return Response(json.dumps({"service": service_name, "state": unit.Unit.ActiveState.decode(), "substate": unit.Unit.SubState.decode()}), status_code=200, media_type='application/json')
        else:
            return Response(json.dumps({'error': 'BAD_REQUEST'}), status_code=403, media_type='application/json')
    except DBusFileNotFoundError:
        return Response(json.dumps({'error': 'BAD_REQUEST'}), status_code=403, media_type='application/json')
