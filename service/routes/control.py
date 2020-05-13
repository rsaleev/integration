
from enum import Enum
from typing import Optional
from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
import json
from pydantic import BaseModel, ValidationError, validator
import configuration as cfg
import asyncio
from datetime import datetime, timedelta
from zeep.exceptions import TransportError, LookupError
from zeep.exceptions import Error as ClientError
import service.settings as ws
from uuid import uuid4
from starlette.background import BackgroundTasks
import dateutil.parser as dp

router = APIRouter()

name = "control"


class CommandRequest(BaseModel):
    type: str = "command"
    error: Optional[int]
    date_event: str
    came_device_id: int
    devic_ip: Optional[str]
    device_type: Optional[int]
    command_number: int
    device_events_id: Optional[int]

    @validator('date_event')
    def date_validator(cls, v):
        dt = dp.parse(v)
        return dt


class CommandResponse(BaseModel):
    type: str = "command"
    error: int = 0
    device_type: Optional[str]
    came_device_id: str
    device_events_id: Optional[str]
    date_event: str

    @validator('date_event', pre=True)
    def date_validator(cls, v):
        dt = v.strftime('%d-%m-%Y %H:%M:%S')
        return dt


class CommandType(Enum):
    OPEN_BARRIER = 3
    CLOSE_BARRIER = 6
    LOCK_BARRIER = 9
    UNLOCK_BARRIER = 12
    TURN_OFF = 15
    TURN_ON = 18
    REBOOT = 25
    CLOSEDOFF = 41
    CLOSEDALL = 40
    OPENALL = 30
    OPENALLOFF = 31


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


async def open_barrier(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'OPENED' or adv_status['statusVal'] == 'LOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'OPEN':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='open')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='open')
                return result
            else:
                return False


async def close_barrier(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'LOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'CLOSE':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='close')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='close')
                return result
            else:
                return False


async def lock_barrier(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'LOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'LOCK':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopen')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopen')
                return result
            else:
                return False


async def unlock_barrier(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'UNLOCKED' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'UNLOCK':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopenoff')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopenoff')
                return result
            else:
                return False


async def turnoff_device(device, request_dt):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'TURN_OFF':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maintenanceon')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maintenanceon')
                return result
            else:
                return False


async def turnon_device(device, request_dt):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'TURN_ON':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maintenanceoff')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maiintenanceoff')
                return result
            else:
                return False


async def reboot_device(device, request_dt):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'REBOOT' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'REBOOT':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='rebootsw')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='rebootsw')
                return result
            else:
                return False


async def lock_barrier_opened(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'OPENALL' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'OPENALL':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='openall')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='openall')
                return result
            else:
                return False


async def unlock_barrier_opened(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'OPENED' or adv_status['statusVal'] == 'OPENALL_OFF' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'OPENALL_OFF':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='openalloff')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='openalloff')
                return result
            else:
                return False


async def lock_barrier_closed(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'CLOSEDALL' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'UNLOCK':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedall')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedall')
                return result
            else:
                return False


async def unlock_barrier_closed(device, request_dt):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.dbconnector_is.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if status['statusVal'] == 'CLOSED' or adv_status['statusVal'] == 'CLOSEDALL_OFF' or gate_status['statusVal'] == 'OUT_OF_SERVICE' or network_status['statusVal'] == 'OFFLINE':
        return False
    else:
        if last_command['stCodename'] != 'UNLOCK':
            result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedalloff')
            return result
        else:
            if last_command['statusTs'] + timedelta(seconds=5) != request_dt:
                result = await ws.soapconnector.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedalloff')
                return result
            else:
                return False


@router.get('/api/integration/v1/control')
async def rem_show():
    pass


@router.post('/api/integration/v1/control')
async def rem_exec(*, request: CommandRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict(exclude_unset=True))
    device = await ws.dbconnector_is.callproc('is_devices_get', rows=1, values=[request.came_device_id, None, None, None, None])
    if not device is None:
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": request.dict(exclude_unset=True)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
        try:
            if request.command_number == 3:
                if device['terType'] in [1, 2]:
                    result = await open_barrier(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])

                        response.date_event = datetime.now()
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
            elif request.command_number == 6:
                if device['terType'] in [1, 2]:
                    result = await close_barrier(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        response.date_event = datetime.now()
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
            elif request.command_number == 9:
                if device['terType'] in [1, 2]:
                    result = await lock_barrier(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
            elif request.command_number == 12:
                if device['terType'] in [1, 2]:
                    result = await unlock_barrier(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
            elif request.command_number == 15:
                result = await turnoff_device(device, request.date_event)
                if result:
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                else:
                    response.error = 1
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
            elif request.command_number == 18:
                result = await turnon_device(device, request.date_event)
                if result:
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
            elif request.command_number == 25:
                result = await reboot_device(device, request.date_event)
                if result:
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                else:
                    response.error = 1
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
            elif request.command_number == 30:
                if device['terType'] in [1, 2]:
                    result = await lock_barrier_opened(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
            elif request.command_number == 31:
                if device['terType'] in [1, 2]:
                    result = await unlock_barrier_opened(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
            elif request.command_number == 40:
                if device['terType'] in [1, 2]:
                    result = await lock_barrier_closed(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)

                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
            elif request.command_number == 41:
                if device['terType'] in [1, 2]:
                    result = await unlock_barrier_closed(device, request.date_event)
                    if result:
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.now()
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                             json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
        except Exception as e:
            response.error = 1
            tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": repr(e)})
            tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                     json.dumps({'uid': str(uid), 'response': repr(e)}, ensure_ascii=False, default=str), datetime.now()])
            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
