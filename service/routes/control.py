
from fastapi.routing import APIRouter
from starlette.responses import Response
import json
from pydantic import BaseModel, ValidationError, validator
import re
from aiomysql import DatabaseError, DataError, OperationalError, ProgrammingError
import configuration as cfg
from zeep.asyncio.transport import AsyncTransport
from zeep.client import Client
import asyncio
from datetime import datetime
from configuration import soap_username, soap_password, soap_url, site_id, soap_timeout
from zeep.client import Client, Document
from zeep.asyncio.transport import AsyncTransport
import io

router = APIRouter()


class RemoteControl:

    def __init__(self):
        self.devices = list
        self.client = object

    async def reboot(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='reboot',
                                                                   sDescription='Reboot device')
            return rc['rSuccess']
        except Exception as e:
            raise e

    async def turn_on(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='maintenanceoff',
                                                                   sDescription='Turn on device')
            return rc['rSuccess']
        except Exception as e:
            raise e

    async def turn_off(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='maintenanceon',
                                                                   sDescription='Turn off device')
            return rc['rSuccess']
        except Exception as e:
            raise e

    # barrier commands

    async def open_barrier(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='open',
                                                                   sDescription='Open barrier')
            return rc['rSuccess']
        except Exception as e:
            raise e

    async def close_barrier(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='close',
                                                                   sDescription='Close barrier')
            return rc['rSuccess']
        except Exception as e:
            raise e

    async def open_and_lock_barrier(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='lockedopen',
                                                                   sDescription='Open and lock barrier')
            return rc['rSuccess']
        except Exception as e:
            raise e

    async def unlock_barrier(self, device_id):
        try:
            terminal_id = next([d['terId'] for d in self.__devices if ['amppId'] == device_id])
            self.client.deviceid = terminal_id
            rc = await self.__client.service.setDeviceStatusHeader(sHeader=self.client.header,
                                                                   sStatus='lockedopenoff',
                                                                   sDescription='Unlock barrier')
            return rc['rSuccess']
        except Exception as e:
            raise e


class CommandRequest(BaseModel):
    type: str
    error: int = 0
    date_event: datetime
    device_number: Optional[int]
    device_ip: Optional[str]
    device_type: Optional[int]
    command_number: Optional[int]
    device_events_id: Optional[int]
    parking_number: Optional[int]

    @validator('date_event')
    def str_to_datetime(cls, v):
        return dp.parse(v)


class CommandResponse(BaseModel):
    type: str
    error: int
    date_event: str
    device_number: Optional[str]
    device_ip: Optional[str]
    device_type: Optional[str]
    command_number: Optional[str]
    device_events_id: Optional[str]
    parking_number: Optional[str]

    @validator('date_event')
    def datetime_to_str(cls, v):
        return datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')


class CommandType(Enum):
    OPEN_BARRIER = 3
    CLOSE_BARRIER = 6
    LOCK_BARRIER = 9
    UNLOCK_BARRIER = 9
    TURN_OFF = 15
    TURN_ON = 18
    REBOOT = 25


@router.post('/rest/control', response_model=CommandResponse):
async def rem_control(rc, command: CommandRequest):
    uid = uuid4()
    if command.device_number in [da['amppId'] for da in rc.devices]:
        try:
            device_id = next(dw['terId'] for dw in rc.devices if dw['amppId'] == command.device_number)
            tasks = BackgroundTasks()
            try:
                if command.command_number == 3:
                    result = await rc.open_barrier(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'open barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 6:
                    result = await rc.close_barrier(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation":  CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 9:
                    result = await rc.lock_barrier(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 12:
                    result = await rc.unlock_barrier(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 15:
                    result = await rc.turn_off(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), default=st, status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 18:
                    result = await rc.turn_on(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
                elif command.command_number == 25:
                    result = await rc.reboot(command.device_number)
                    if result:
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'operation': 'close barrier', 'request': command.dict()}), datetime.now()])
                        return Response(json.dumps(command, default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        command.error = 1
                        return JSONResponse(command, status_code=200, media_type='application/json')
            except Exception as e:
                tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
                tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                    {'uid': str(uid), 'error': repr(e), 'request': command.dict()}), datetime.now()])
                return Response(json.dumps({'error': 'INTERNAL ERROR'}), status_code=500, media_type='application/json')
        except KeyError as e:
            tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(command.command_number).name, "request": json.dumps(command.dict())})
            tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                {'uid': str(uid), 'error': repr(e), 'request': command.dict()}), datetime.now()])
            return Response(json.dumps({'error': 'BAD REQUEST', 'comment': 'device not found'}), status_code=403, media_type='application/json')
