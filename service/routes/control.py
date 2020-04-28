
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


@router.get('/rest/control')
async def rem_control_info():
    JSONResponse


@router.post('/rest/control')
async def rem_control(*, request: CommandRequest):
    uid = uuid4()
    tasks = BackgroundTasks()
    response = CommandResponse(**request.dict(exclude_unset=True))
    device = next((d for d in ws.devices if d['terId'] == request.came_device_id), None)
    if not device is None:
        ws.soapconnector.deviceid = device['terAddress']
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(request.command_number).name, "request": request.dict(exclude_unset=True)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info',
                                                                                 json.dumps({'uid': str(uid),  'request': request.dict(exclude_unset=True)}, ensure_ascii=False), datetime.now()])
        try:
            if request.command_number == 3:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='open')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid),  'response': request.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 6:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='close')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation":  CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 9:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='lockedopen')
                response.error = 0
                if result:
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 12:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='lockedopenoff')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': request.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 15:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='maintenanceon')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 18:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='maintenanceoff')
                response.error = 0
                if result:
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
                else:
                    response.error = 1
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
                    tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'info', json.dumps(
                        {'uid': str(uid), 'response': response.dict(exclude_unset=True)}), datetime.now()])
                    return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
            elif request.command_number == 25:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='rebootsw')
                response.error = 0
                if result:
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
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
            elif request.command_number == 30:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='closedoff')
                response.error = 0
                if result:
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
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
            elif request.command_number == 31:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='closedall')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
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
            elif request.command_number == 32:
                result = await ws.soapconnector.client.service.SetDeviceStatusHeader(sHeader=ws.soapconnector.header, sStatus='allout')
                if result:
                    response.error = 0
                    response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
                    tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
                        request.command_number).name, "response": response.dict(exclude_unset=True)})
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

        except (TransportError, TimeoutError, ClientError, asyncio.TimeoutError):
            # reconnect to service
            await ws.soapconnector.connect()
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
    else:
        response.error = 1
        response.date_event = datetime.strftime(datetime.now(), '%d-%m-%Y %H:%M:%S')
        tasks.add_task(ws.logger.info, {"module": name, "uid": str(uid), "operation": CommandType(
            request.command_number).name, "response": response.dict(exclude_unset=True)})
        tasks.add_task(ws.dbconnector_is.callproc, 'is_log_ins', rows=0, values=[name, 'error', json.dumps(
            {'uid': str(uid), 'error': f'Device {request.device_number} not found'}), datetime.now()])
        request.error = 1
        return Response(json.dumps(response.dict(exclude_unset=True), ensure_ascii=False), status_code=200, media_type='application/json', background=tasks)
