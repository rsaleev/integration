
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

name = "webservice_control"


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
    date_event: str
    device_number: Optional[int]
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
        if not v in ['command', 'places']:
            raise ValidationError
        else:
            return v


class CommandResponse(BaseModel):
    type: str
    error: str
    device_type: Optional[str]
    device_number: Optional[str]
    device_events_id: Optional[str]
    date_event: str
    parking_number: Optional[str]


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
    print(ws.devices)
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict(exclude_unset=True))
    try:
        if request.device_number in [d['amppId'] for d in ws.devices] or request.device_number in [d['terAddress'] for d in ws.devices]:
            tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": json.dumps(request.dict(exclude_unset=True))})
            tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                     json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False), datetime.now()])
            # define device id for request
            device_id = next((d['terAddress'] for d in ws.devices if ['amppId'] == request.device_number), request.device_number)
            try:
                if request.command_number == 3:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='open')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid),  'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 6:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='close')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation":  CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 9:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='lockedopen')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 12:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='lockedopenoff')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 15:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='maintenanceon')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 18:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='maintenanceoff')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                elif request.command_number == 25:
                    ws.soapconnector.deviceid = device_id
                    result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='rebootsw')
                    if result:
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                    else:
                        response.error = 1
                        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                            request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False, default=str)})
                        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                            {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            except (TransportError, TimeoutError, ClientError):
                # reconnect to service
                await ws.soapconnector.connect()
        else:
            response.error = 1
            response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
            tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                request.command_number).name, "response": json.dumps(response.dict(exclude_unset=True), ensure_ascii=False)})
            tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
                {'uid': str(uid), 'error': f'Device {request.device_number} not found'}), datetime.now()])
            request.error = 1
            return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
    except KeyError as e:
        response.error = 1
        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "error": repr(e)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
            {'uid': str(uid), 'error': repr(e), }), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True)), status_code=503, media_type='application/json', background=tasks)
    except ValidationError as e:
        response.error = 1
        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "error": repr(e)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
            {'uid': str(uid), 'error': repr(e)}), datetime.now()])
        return Response(json.dumps(response.dict(exclude_unset=True)), status_code=503, media_type='application/json', background=tasks)
