
from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
import json
from pydantic import BaseModel, ValidationError, validator
import configuration as cfg
import asyncio
from datetime import datetime
from zeep.exceptions import TransportError, LookupError
from zeep.exceptions import Error as ClientError
from service import settings as ws
from typing import Optional
from enum import Enum


router = APIRouter()


class DeviceStatus():
    def __init__(self, command_num, device_id, device_ip, device_type, ampp_id, ampp_type, dt):
        self.codename = 'Command'
        self.__value: int = command_num
        self.__device_id: int = None
        self.__device_ip: str = None
        self.__device_type: int = None
        self.__ampp_id: int = None
        self.__ampp_type: int = None
        self.__ts = dt


class CommandRequest(BaseModel):
    type: str
    error: int
    date_event: datetime
    device_number: Optional[int]
    came_device_id: Optional[int]
    device_ip: Optional[str]
    device_type: Optional[int]
    command_number: Optional[int]
    device_events_id: Optional[int]
    parking_number: Optional[int]
    client_free: Optional[int]
    client_busy: Optional[int]
    vip_client_free: Optional[int]
    vip_client_busy: Optional[int]

    @validator('type')
    def type_validation(cls, v):
        if (v == 'command' and device_number is None or came_device_id is None or command_number is None):
            raise ValidationError
        elif (v == 'places' and client_free is None or vip_client_free is None or client_busy is None or vip_client_busy is None):
            raise ValidationError


class CommandResponse(BaseModel):
    type: str
    error: str
    device_type: Optional[str]
    device_number: Optional[str]
    device_events_id: Optional[str]
    date_event: str
    parking_number: Optional[str]

    @validator('date_event')
    def str_to_datetime(cls, v):
        return dp.parse(v)

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


@router.post('/rest/control')
async def rem_control(*, request: CommandRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict(exclude_unset=True))
    try:
        if (request.device_number in [da['amppId'] for da in ws.devices] or request.device_number in [dw['terAddress'] for dw in ws.devices]):
            tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": json.dumps(request.dict(exclude_unset=True))})
            tasks.add_task(tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                                    json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}), datetime.now()]))
            # define device id for request
            ws.deviceid = next([d['terAddress'] for d in self.__devices if ['amppId'] == device_id], request.device_number)
            try:
                if request.command_number == 3:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='open')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(app.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid),  'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 6:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='close')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation":  CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 9:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='lockedopen')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 12:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='lockedopenoff')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 15:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='maintenanceon')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 18:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='maintenanceoff')
                    if cmd['rSuccess']:
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 25:
                    cmd = await ws.soapconnector.client.SetDeviceStatusHeader(sHeader=ws.header, sStatus='maintenanceon')
                    if cmd['rSuccess']:
                        respnse.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.now()
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
            except (TransportError, TimeoutError, ClientError):
                # reconnect to service
                await ws.soapconnector.connect()
        else:
            response.error = 1
            response.date_event = datetime.now()
            tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True))})
            tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
                {'uid': str(uid), 'error': f'Device {command.device_number} not found'}), datetime.now()])
            request.error = 1
            return Response(json.dumps(response.dict(exclude_unset=True), default=str), status_code=200, media_type='application/json', background=tasks)
    except KeyError as e:
        response.error = 1
        response.date_event = datetime.now()
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "error": repr(e)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
            {'uid': str(uid), 'error': repr(e), }), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True)), status_code=503, media_type='application/json', background=tasks)
    except ValidationError as e:
        response.error = 1
        response.date_event = datetime.now()
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "error": repr(e)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
            {'uid': str(uid), 'error': repr(e)}), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True)), status_code=503, media_type='application/json', background=tasks)
