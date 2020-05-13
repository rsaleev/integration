
from enum import Enum
from typing import Optional
from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
import json
from pydantic import BaseModel, ValidationError, validator
import configuration as cfg
import asyncio
from datetime import datetime
from zeep.exceptions import TransportError, LookupError
from zeep.exceptions import Error as ClientError
import service.settings as ws
from uuid import uuid4
from starlette.background import BackgroundTasks


router = APIRouter()

name = "control"


class CommandRequest(BaseModel):
    type: str = "command"
    error: Optional[int]
    date_event: str
    came_device_id: int
    device_ip: Optional[str]
    device_type: Optional[int]
    command_number: int
    device_events_id: Optional[int]


class CommandResponse(BaseModel):
    type: str = "command"
    error: Optional[str]
    device_type: Optional[str]
    came_device_id: str
    device_events_id: Optional[str]
    date_event: str


class CommandType(Enum):
    OPEN_BARRIER = 3
    CLOSE_BARRIER = 6
    LOCK_BARRIER = 9
    UNLOCK_BARRIER = 12
    TURN_OFF = 15
    TURN_ON = 18
    REBOOT = 25
    CLOSEDOFF = 30
    CLOSEDALL = 31
    OPENALL = 32
    OPENALLOFF = 33


class CommandStatus:
    def __init__(self, device: dict, command: int, result: bool):
        self.__ampp_id = device['amppId']
        self.__ampp_type = device['amppType']
        self.__codename = 'Command'
        self.__command = command
        self.__ts = int(datetime.now().timestamp())
        self.__device_ip = device['terIp']
        self.__device_id = device['terId']
        self.__device_type = device['terType']
        self.__device_address = device['terAddress']
        self.__result = result

    @property
    def value(self):
        return self.__value

    @value.getter
    def value(self):
        if self.__value == 3 and self.__result:
            return 'MANUAL_OPEN'
        elif self.__value == 3 and not self.__result:
            return 'ALREADY_OPENED'
        elif self.__value == 6 and self.__result:
            return 'MANUAL_CLOSE'
        elif self.__value == 6 and not self.__result:
            return 'ALREADY_CLOSED'
        elif self.__value == 9 and self.__result:
            return 'LOCKED'
        elif self.__value == 9 and not self.__result:
            return 'ALREADY_LOCKED'
        elif self.__value == 12 and self.__result:
            return 'UNLOCK'
        elif self.__value == 12 and not self.__result:
            return 'ALREADY_UNLOCKED'
        elif self.__value == 15 and self.__result:
            return 'TURN_OFF'
        elif self.__value == 15 and not self.__result:
            return 'ALREADY_TURNED_OFF'
        elif self.__value == 18 and self.__result:
            return 'TURN_ON'
        elif self.__value == 18 and not self.__result:
            return 'ALREADY_TURNED_OFF'
        elif self.__value == 101 and self.__result:
            return 'CHALLENGED_IN'
        elif self.__value == 101 and not self.__result:
            return 'ALREADY_OPENED'
        elif self.__value == 102 and self.__result:
            return 'CHALLENGED_OUT'
        elif self.__value == 102 and not self.__result:
            return 'ALREADY_OPENED'
        elif self.__value == 0 and self.__result:
            return 'PLACES_UPDATED'
        elif self.__value == 0 and not self.__result:
            return 'PLACES_NOT_UPDATED'

    @property
    def instance(self):
        return {'device_id': self.__device_id,
                'device_address': self.__device_address,
                'device_type': self.__device_type,
                'codename': self.__codename,
                'value': self.value,
                'ts': self.__ts,
                'ampp_id': self.__ampp_id,
                'ampp_type': self.__ampp_type,
                'device_ip': self.__device_ip}


async def open_barrier(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='open')
    return result


async def close_barrier(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='close')
    return result


async def lock_barrier(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='lockedopen')
    return result


async def unlock_barrier(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='lockedopenoff')
    return result


async def turnoff_device(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='maintenanceon')
    return result


async def turnon_device(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='maintenanceoff')
    return result


async def reboot_device(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='rebootsw')
    return result


async def lock_barrier_opened(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='openall')
    return result


async def unlock_barrier_opened(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='openalloff')
    return result


async def lock_barrier_closed(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='closedall')
    return result


async def unlock_barrier_closed(device_address):
    result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device_address, sStatus='closedalloff')
    return result


@router.get('/api/integration/v1/control')
async def rem_show():
    pass


@router.post('/api/integration/v1/control')
async def rem_exec(*, request: CommandRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict(exclude_unset=True))
    device = next((d for d in ws.devices if d['terId'] == request.came_device_id), None)
    if not device is None:
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": request.dict(exclude_unset=True)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False), datetime.now()])
        try:
            if request.command_number == 3:
                if device['terType'] in [1, 2]:
                    # check barrier statuses
                    check_tasks = []
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
                    status, adv_status, gate_status = await asyncio.gather(*tasks)
                    if status['statusVal'] == 'OPENED' or adv_status['statusVal'] == 'LOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE':
                        response.error = 1
                        return Response(json.dumps(response.dict(), default=str), media_type='application/json', background=tasks)
                    else:
                        result = await open_barrier(device['terAddress'])
                        if result:
                            return response
                        else:
                            response.error = 0
                            return response
                else:
                    response.error = 0
                    return response
            elif request.command_number == 6:
                if device['terType'] in [1, 2]:
                    # check barrier statuses
                    check_tasks = []
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
                    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
                    status, adv_status, gate_status = await asyncio.gather(*tasks)
                    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'LOCKED_FOR_TRANSIT' or gate_status['statusVal'] == 'OUT_OF_SERVICE':
                        response.error = 1
                        return response
                    else:
                        result = await close_barrier(device['terAddress'])
                        if result:
                            return response
                        else:
                            response.error = 0
                            return response
                else:
                    response.error = 1
                    return response
            elif request.command_number == 9:
                # check barrier statuses
                check_tasks = []
                check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
                check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
                adv_status, gate_status = await asyncio.gather(*tasks)
                if adv_status['statusVal'] == 'LOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE':
                    response.error = 1
                    return response
                else:
                    result = await lock_barrier(device['terAddress'])
                    if result:
                        return response
                    else:
                        response.error = 0
                        return response
            elif request.command_number == 12:
                # check barrier statuses
                check_tasks = []
                check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
                check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
                adv_status, gate_status = await asyncio.gather(*tasks)
                if adv_status['statusVal'] == 'UNLOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE':
                    response.error = 1
                    return response
                else:
                    result = await unlock_barrier(device['terAddress'])
                    if result:
                        return response
                    else:
                        response.error = 0
                        return response
            elif request.command_number == 15:
                general_status = ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General'])
                general_status = await asyncio.gather(*tasks)
                if adv_status['statusVal'] == 'UNLOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE':
                    response.error = 1
                    return response
                else:
                    result = await unlock_barrier(device['terAddress'])
                    if result:
                        return response
                    else:
                        response.error = 0
                        return response
