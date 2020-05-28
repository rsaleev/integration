
from enum import Enum
from typing import Optional
from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
import json
from pydantic import BaseModel, ValidationError, validator
import configuration.settings as cs
import asyncio
from datetime import datetime, timedelta
from zeep.exceptions import TransportError, LookupError
from zeep.exceptions import Error as ClientError
import integration.service.settings as ws
from uuid import uuid4
from starlette.background import BackgroundTasks
import dateutil.parser as dp

router = APIRouter()

name = "cmiu.control"


class CommandRequest(BaseModel):
    type: str
    error: int
    date_event: str
    came_device_id: int
    device_ip: Optional[str] = None
    device_type: Optional[int] = None
    command_number: int
    device_events_id: Optional[int] = None

    @validator('date_event')
    def date_validator(cls, v):
        dt = dp.parse(v)
        return dt


class CommandResponse(BaseModel):
    type: str
    error: int = 0
    device_type: int
    came_device_id: int
    device_events_id: int
    date_event: str

    @validator('date_event', pre=True)
    def date_validator(cls, v):
        return datetime.now().strftime('%d-%m-%Y %H:%M:%S')


class PlacesResponse(BaseModel):
    type: "places"
    parking_number: int
    date_event: str
    error: int


class CommandType(Enum):
    MANUALLY_OPEN = 3
    MANUALLY_CLOSE = 6
    LOCK = 9
    UNLOCK = 12
    TURN_OFF = 15
    TURN_ON = 18
    REBOOT = 25
    CLOSEDOFF = 42
    CLOSED = 41
    CLOSEDALL = 40
    OPENALL = 30
    OPENALLOFF = 31
    TRAFFIC_JAM_ON = 81
    TRAFFIC_JAM_OFF = 82
    CHALLENGED_IN = 101
    CHALLENGED_OUT = 102
    CHALLENGED_OUT_SIM = 103

    @staticmethod
    def list():
        return list(map(lambda c: {'number': c.value, 'description': c.name}, CommandType))


class CommandStatus:
    def __init__(self, device: dict, codename: str, value: str):
        self.__device_id = device['terId']
        self.__ampp_id = device['amppId']
        self.__ampp_type = device['amppType']
        self.__codename = codename
        self.__value = value
        self.__ts = int(datetime.now().timestamp())
        self.__device_ip = device['terIp']
        self.__device_type = device['terType']
        self.__device_address = device['terAddress']
        self.__act_uid = uuid4()

    @property
    def instance(self):
        return {'device_id': self.__device_id,
                'device_address': self.__device_address,
                'device_type': self.__device_type,
                'codename': self.__codename,
                'value': self.__value,
                'ts': self.__ts,
                'ampp_id': self.__ampp_id,
                'ampp_type': self.__ampp_type,
                'device_ip': self.__device_ip,
                'act_uid': str(self.__act_uid)}


async def open_barrier(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks, return_exceptions=True)
    # check status and generate event for CMIU
    if status['statusVal'] == 'OPENED':
        report_status = CommandStatus(device, 'Barrier', 'ALREADY_OPENED')
        try:
            await asyncio.wait_for(ws.amqpconnector.send(report_status.instance, persistent=True, keys=['active.barrier'], priority=10), timeout=0.5)
        except:
            pass
        return False
    elif (adv_status['statusVal'] == 'LOCKED' or
          gate_status['statusVal'] == 'OUT_OF_SERVICE' or
          network_status['statusVal'] == 'OFFLINE' or
          last_command['statusVal'] == 'OPEN' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        tasks = []
        tasks.append(ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='open'))
        report_status_pre_execution = CommandStatus(device, 'Command', CommandType(request.command_number).name)
        tasks.append(ws.amqpconnector.send(report_status_pre_execution.instance, persistent=True, keys=['command.entry.barrier', 'active.barrier'], priority=10))
        futures = await asyncio.gather(*tasks)
        result = futures[0]
        if result:
            report_status_post_execution = CommandStatus(device, 'BarrierStatus', 'MANUALLY_OPENED')
            ws.amqpconnector.send(report_status_pre_execution.instance, persistent=True, keys=['command.entry.barrier', 'active.barrier'], priority=10)
        return result


async def close_barrier(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'CLOSED' or
        adv_status['statusVal'] == 'LOCKED' or
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'CLOSE' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='close')
        return result


async def lock_barrier(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'OPENED' or
        adv_status['statusVal'] == 'LOCKED' or
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'LOCK' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopen')
        return result


async def unlock_barrier(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'OPENED' or
        adv_status['statusVal'] == 'LOCKED' or
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'UNLOCK' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='lockedopenoff')
        return result


async def turnoff_device(device, request):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'TURN_OFF' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maintenanceon')
        return result


async def turnon_device(device, request):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'IN_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'TURN_ON' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='maintenanceoff')
        return result


async def reboot_device(device, request):
    # check device['terId'] status
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'REBOOT' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'REBOOT' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='rebootsw')
        return result


async def lock_barrier_opened(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'OPENED' or
        adv_status['statusVal'] == 'LOCKED' or
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'OPENALL' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='allout')
        return result


async def unlock_barrier_opened(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (status['statusVal'] == 'OPENED' or
        adv_status['statusVal'] == 'UNLOCKED' or
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'OPENALLOFF' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='alloutoff')
        return result


async def block_casual_transit(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'OPENALLOFF' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closed')
        return result


async def block_all_transit(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'OPENALLOFF' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedall')
        return result


async def unblock_transit(device, request):
    # check barrier statuses
    check_tasks = []
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
    check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
    status, adv_status, gate_status, last_command, network_status = await asyncio.gather(*check_tasks)
    if (
        gate_status['statusVal'] == 'OUT_OF_SERVICE' or
        network_status['statusVal'] == 'OFFLINE' or
            last_command['statusVal'] == 'OPENALLOFF' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
        return False
    else:
        await ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', CommandType(request.command_number).name, datetime.now()])
        result = await ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='closedoff')
        return result


async def challenged_in(device, request):
    if device['terType'] == 1:
        # check barrier statuses
        check_tasks = []
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[0, 'Command']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_device_get', rows=1, values=[0, None, None, None, None]))
        status, adv_status, gate_status, last_command, network_status, last_server_command, device_server = await asyncio.gather(*check_tasks, return_exceptions=True)
        # check status and generate event for CMIU
        if status['statusVal'] == 'OPENED':
            report_status = CommandStatus(device, 'Barrier', 'ALREADY_OPENED')
            try:
                await asyncio.wait_for(ws.amqpconnector.send(report_status.instance, persistent=True, keys=['active.barrier'], priority=10), timeout=0.5)
            except:
                pass
            return False
        elif (adv_status['statusVal'] == 'LOCKED' or
              gate_status['statusVal'] == 'OUT_OF_SERVICE' or
              network_status['statusVal'] == 'OFFLINE' or
              last_command['statusVal'] == 'OPEN' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
            return False
        else:
            tasks = []
            tasks.append(ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='open'))
            tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', 'MANUALLY_OPEN', datetime.now()]))
            tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[0, 'Command', CommandType(request.command_number).name, datetime.now()]))
            report_status_barrier = CommandStatus(device, 'Command', 'MANUALLY_OPEN')
            report_status_server = CommandStatus(device_server, 'Command', CommandType(request.command_number).name)
            tasks.append(ws.amqpconnector.send(report_status_barrier.instance, persistent=True, keys=['active.barrier', 'command.challenged.in'], priority=10))
            result, _, _, _ = await asyncio.gather(*tasks)
            return result
    else:
        return False


async def challenged_out(device, request):
    if device['terType'] == 2:
        # check barrier statuses
        check_tasks = []
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierStatus']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'BarrierAdvancedStatus']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'General']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Command']))
        check_tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_get', rows=1, values=[device['terId'], 'Network']))
        check_tasks.append(ws.DBCONNECTOR_IS.callrpoc('is_status_get', rows=1, values=[0, 'Command']))
        check_tasks.append(ws.DBCONNECTOR_IS.callrpco('is_device_get', rows=1, values=[9, None, None, None, None]))
        status, adv_status, gate_status, last_command, network_status, last_server_command, device_server = await asyncio.gather(*check_tasks, return_exceptions=True)
        # check status and generate event for CMIU
        if status['statusVal'] == 'OPENED':
            report_status = CommandStatus(device, 'Barrier', 'ALREADY_OPENED')
            try:
                await asyncio.wait_for(ws.amqpconnector.send(report_status.instance, persistent=True, keys=['active.barrier'], priority=10), timeout=0.5)
            except:
                pass
            return False
        elif (adv_status['statusVal'] == 'LOCKED' or
              gate_status['statusVal'] == 'OUT_OF_SERVICE' or
              network_status['statusVal'] == 'OFFLINE' or
              last_command['statusVal'] == 'OPEN' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)
              and last_server_command['statusVal'] == 'CHALLENGED_IN' and (last_command['statusTs'] is None or last_command['statusTs'] + timedelta(seconds=5) < request.date_event)):
            return False
        else:
            tasks = []
            tasks.append(ws.SOAPCONNECTOR.execute('SetDeviceStatusHeader', header=True, device=device['terAddress'], sStatus='open'))
            tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[device['terId'], 'Command', 'MANUALLY_OPEN', datetime.now()]))
            tasks.append(ws.DBCONNECTOR_IS.callproc('is_status_upd', rows=0, values=[0, 'Command', CommandType(request.command_number).name, datetime.now()]))
            report_status_barrier = CommandStatus(device, 'Command', 'OPEN')
            report_status_server = CommandStatus(device_server, 'Command', CommandType(request.command_number).names)
            tasks.append(ws.amqpconnector.send(report_status_barrier.instance, persistent=True, keys=['active.barrier', 'comamnd.challenged.in'], priority=10))
            result, _, _, _, _ = await asyncio.gather(*tasks)
            return result
    else:
        return False


async def challenged_out_simulate(device, request):
    pass


@router.post('/api/cmiu/v2/control')
async def ccom_exec(*, request: CommandRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict())
    try:
        if request.type == "command":
            device = await ws.DBCONNECTOR_IS.callproc('is_device_get', rows=1, values=[request.came_device_id, None, None, None, None])
            print(device)
            if not device is None:
                tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": request.dict(exclude_unset=True)})
                tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                         json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                if request.command_number == 3:
                    if device['terType'] in [1, 2]:
                        result = await open_barrier(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])

                            response.date_event = datetime.now()
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            response.date_event = datetime.now()
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                elif request.command_number == 6:
                    if device['terType'] in [1, 2]:
                        result = await close_barrier(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            response.date_event = datetime.now()
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            response.date_event = datetime.now()
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                elif request.command_number == 9:
                    if device['terType'] in [1, 2]:
                        result = await lock_barrier(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 12:
                    if device['terType'] in [1, 2]:
                        result = await unlock_barrier(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 15:
                    result = await turnoff_device(device, request)
                    if result:
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                    else:
                        response.error = 1
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                elif request.command_number == 18:
                    result = await turnon_device(device, request)
                    if result:
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                elif request.command_number == 25:
                    result = await reboot_device(device, request)
                    if result:
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)

                    else:
                        response.error = 1
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                elif request.command_number == 30:
                    if device['terType'] in [1, 2]:
                        result = await lock_barrier_opened(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 31:
                    if device['terType'] in [1, 2]:
                        result = await unlock_barrier_opened(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 40:
                    if device['terType'] in [1, 2]:
                        result = await block_all_transit(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 41:
                    if device['terType'] in [1, 2]:
                        result = await block_casual_transit(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                elif request.command_number == 42:
                    if device['terType'] in [1, 2]:
                        result = await unblock_transit(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])

                elif request.command_number == 81:
                    pass

                elif request.command_number == 101:
                    if device['terType'] in [1, 2]:
                        result = await challenged_in(device, request)
                        if result:
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                        else:
                            response.error = 1
                            tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                            tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                     json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])
                            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": response.dict(exclude_unset=True)})
                        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                 json.dumps({'uid': str(uid), 'response': response.dict(exclude_unset=True)}, ensure_ascii=False, default=str), datetime.now()])

    except Exception as e:
        raise e
        response.error = 1
        tasks.add_task(ws.LOGGER.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": repr(e)})
        tasks.add_task(ws.DBCONNECTOR_IS.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid), 'response': repr(e)}, ensure_ascii=False, default=str), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=403, media_type='application/json', background=tasks)
