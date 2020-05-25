from fastapi.routing import APIRouter
from starlette.responses import Response
from starlette.background import BackgroundTasks
import json
import configuration as cfg
import service.settings as ws
import asyncio
from pydantic import BaseModel, validator
from typing import Optional
from itertools import groupby
import re


router = APIRouter()


class DeviceRequestConfig(BaseModel):
    terminal_address: Optional[int] = None
    terminal_type: Optional[int] = None
    terminal_description: Optional[str] = None
    ampp_id: Optional[int] = None
    ampp_type: Optional[int] = None
    terminal_area_id: Optional[int] = None
    terminal_ip: Optional[int] = None
    cashbox_capacity: Optional[int] = None
    cashbox_limit: Optional[int] = None
    uniteller_id: Optional[str] = None
    uniteller_ip: Optional[str] = None
    payonline_id: Optional[str] = None
    payonline_ip: Optional[str] = None
    imager_ip: Optional[str] = None
    imager_enabled: Optional[int] = None
    cam_plate_ip: Optional[str] = None
    cam_photo_1_ip: Optional[str] = None
    cam_photo_2_ip: Optional[str] = None
    ticket_device: Optional[str] = None


class DeviceRequstStatus(BaseModel):
    status: str
    operation: str


@validator('ampp_id')
def check_ampp_id(cls, v):
    if v % 100 < 100 and v//100 == cfg.ampp_parking_id:
        return v
    else:
        raise ValueError('Incorrect AMPP Device ID format or value')


@validator('ampp_type')
def check_ampp_type(cls, v):
    if v in [1, 2, 3, 4]:
        return v
    else:
        raise ValueError('Incorrect AMPP Device Type value')


@validator('terminal_ip')
def check_terminal_ip(cls, v):
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", v):
        return v
    else:
        raise ValueError('Incorrect Terminal IP format')


@validator('uniteller_ip')
def check_uniteller_ip(cls, v):
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", v):
        return v
    else:
        raise ValueError('Incorrect Uniteller IP format')


@validator('payonline_ip')
def check_payonline_ip(cls, v):
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", v):
        return v
    else:
        raise ValueError('Incorrect Payonline IP format')


@router.get('/api/integration/v1/devices')
async def get_devices():
    data = await ws.dbconnector_is.callproc('is_device_get', rows=-1, values=[None, None, None, None, None])
    return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')


@router.get('/api/integration/v1/device/{ter_id}/configuration')
async def get_configuration(ter_id):
    try:
        tasks = []
        tasks.append(ws.dbconnector_is.callproc('is_column_get', rows=1, values=[ter_id]))
        tasks.append(ws.dbconnector_is.callproc('is_cashier_get', rows=1, values=[ter_id]))
        column, cashier = await asyncio.gather(*tasks)
        if not column is None and cashier is None:
            return Response(json.dumps(column, default=str), status_code=200, media_type='application/json')
        elif column is None and cashier is not None:
            return Response(json.dumps(cashier, default=str), status_code=200, media_type='application/json')
        else:
            resp = {'error': 'BAD REQUEST', 'comment': 'Unknown ID'}
            return Response(json.dumps(resp, default=str), status_code=403, media_type='application/json')
    except Exception as e:
        resp = {'error': 'BAD REQUEST', 'comment': repr(e)}
        return Response(json.dumps(resp, default=str), status_code=403, media_type='application/json')


@router.get('/api/integration/v1/device/{ter_id}/statuses')
async def get_statuses(ter_id):
    data = await ws.dbconnector_is.callproc('is_status_get', rows=-1, values=[ter_id, None])
    if not data is None:
        return Response(json.dumps(data, default=str), status_code=200, media_type='application/json')
    else:
        resp = {'error': 'BAD REQUEST', 'comment': 'Unknown ID'}
        return Response(json.dumps(resp, default=str), status_code=403, media_type='application/json')


@router.post('/api/integration/v1/device/{ter_id}/configuration')
async def modify_device_config(ter_id, params: DeviceRequestConfig):
    try:
        tasks = []
        tasks.append(ws.dbconnector_is.callproc('is_column_get', rows=1, values=[ter_id]))
        tasks.append(ws.dbconnector_is.callproc('is_cashier_get', rows=1, values=[ter_id]))
        column, cashier = await asyncio.gather(*tasks)
        if column:
            await ws.dbconnector_is.callproc('is_column_upd', rows=0, values=[ter_id, params.terminal_address,
                                                                              params.terminal_area_id, params.terminal_type, params.terminal_description, params.ampp_id, params.ampp_type, params.terminal_ip,
                                                                              params.cam_plate_ip, params.cam_photo_1_ip, params.cam_photo_2_ip, params.imager_ip, params.imager_enabled, params.ticket_device])
            return Response(status_code=204, media_type='application/json')
        elif cashier:
            await ws.dbconnector_is.callproc('is_cashier_upd', values=[ter_id, params.terminal_address,
                                                                       params.terminal_area_id, params.terminal_type, params.terminal_description, params.ampp_id, params.ampp_type, params.terminal_ip, params.cashbox_capacity, params.cashbox_limit,
                                                                       params.uniteller_id, params.uniteller_ip, params.payonline_id, params.uniteller_ip, params.imager_ip, params.imager_enabled])
        else:
            data = {'error': 'BAD REQUEST', 'comment': 'Unknown ID'}
            return Response(json.dumps(data, default=str), status_code=403, media_type='application/json')
    except Exception as e:
        data = {'error': 'BAD REQUEST', 'comment': repr(e)}
        return Response(json.dumps(data, default=str), status_code=403, media_type='application/json')


@router.post('/api/integration/v1/device/{ter_id}/statuses')
async def modify_device_statuses(ter_id, params: DeviceRequstStatus):
    try:
        device = await ws.dbconnector_is.callproc('is_device_get', rows=1, values=[ter_id, None, None, None, None])
        if device:
            if params.operation == 'add':
                await ws.dbconnector_is.callproc('is_status_ins', rows=0, values=[ter_id, params.status])
                return Response(status_code=204, media_type='application/json')
            elif params.operation == 'del':
                await ws.dbconnусtor_is.callproc('is_status_del', rows=0, values=[ter_id, params.status])
                return Response(status_code=204, media_type='application/json')
            else:
                data = {'error': 'BAD REQUEST', 'comment': 'Unknown ID'}
                return Response(json.dumps(data, default=str), status_code=403, media_type='application/json')
        else:
            data = {'error': 'BAD REQUEST', 'comment': 'Unknown ID'}
            return Response(json.dumps(data, default=str), status_code=403, media_type='application/json')
    except Exception as e:
        {'error': 'BAD REQUEST', 'comment': repr(e)}
        data = {'error': 'BAD REQUEST', 'comment': repr(e)}
        return Response(json.dumps(data, default=str), status_code=500, media_type='application/json')
