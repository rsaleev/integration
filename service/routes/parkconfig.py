from fastapi.routing import APIRouter
from starlette.responses import Response, JSONResponse
from starlette.background import BackgroundTask
import configuration as cfg
import json
import asyncio
import service.settings as ws
import configparser

router = APIRouter()


async def initialize_info() -> None:
    parser = configparser.RawConfigParser()
    parser.read(cfg.CONFIGURATION)
    object_latitude = parser.getfloat("AMPP", "parking_latitude")
    object_longitude = parser.getfloat("AMPP", "parking_longitude")
    object_address = parser.get("AMPP", "parking_address")
    await ws.dbconnector_is.callproc('is_info_ins', rows=0, values=[cfg.object_id, cfg.ampp_parking_id, object_latitude, object_longitude, object_address])


async def initialize_server() -> None:
    ampp_id_mask = cfg.ampp_parking_id * 100
    service_version = await ws.soapconnector.client.service.GetVersion()
    result = await ws.soapconnector.execute('GetVersion')
    await ws.dbconnector_is.callproc('is_device_ins', rows=0, values=[0, 0, 0, 'server', ampp_id_mask+1, 1, 1, cfg.server_ip, result['rVersion']])


async def initialize_device(device: dict, mapping: list) -> None:
    device_is = next(d for d in mapping if d['ter_id'] == device['terId'])
    ampp_id_mask = cfg.ampp_parking_id * 100
    await ws.dbconnector_is.callproc('is_device_ins', rows=0, values=[device['terId'], device['terAddress'], device['terType'], device_is['description'],
                                                                      ampp_id_mask+device_is['ampp_id'], device_is['ampp_type'], device['terIdArea'],
                                                                      device['terIPV4'], device['terVersion']])
    imager_enabled = 1 if device_is['barcode_reader_enabled'] else 0
    if device['terType'] in [1, 2]:
        if not device['terJSON'] is None and device['terJSON'] != '':
            config = json.loads(device['terJSON'])
            ocr_mode = 'unknown'
            if len(config.items()) > 0:
                if config['CameraMode'] == 1:
                    ocr_mode = 'trigger'
                elif config['CameraMode'] == 0:
                    ocr_mode = 'freerun'
            await ws.dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  ocr_mode, device['terCamPhoto1'],
                                                                              device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                              imager_enabled,
                                                                              ])
        else:
            await ws.dbconnector_is.callproc('is_column_ins', rows=0, values=[device['terId'], device['terCamPlate'],  'unknown', device['terCamPhoto1'],
                                                                              device['terCamPhoto2'], device_is['ticket_device'], device_is['barcode_reader_ip'],
                                                                              imager_enabled,
                                                                              ])

    elif device['terType'] == 3:
        await ws.dbconnector_is.callproc('is_cashier_ins', rows=0, values=[device['terId'], device_is['cashbox_capacity'], device_is['cashbox_limit'], device_is['uniteller_id'],
                                                                           device_is['uniteller_ip'], device_is['payonline_id'], device_is['payonline_ip'],
                                                                           device_is['barcode_reader_ip'], 1 if device_is['barcode_reader_enabled'] else 0])


async def initialize_statuses(devices: list, mapping: dict) -> None:
    tasks = []
    for d_is in mapping:
        if d_is['ter_id'] == 0:
            for st in d_is['statuses']:
                tasks.append(ws.dbconnector_is.callproc('is_status_ins', rows=0, values=[0, st]))
        if d_is['ter_id'] > 0:
            for st in d_is['statuses']:
                tasks.append(ws.dbconnector_is.callproc('is_status_ins', rows=0, values=[d_is['ter_id'], st]))
    await asyncio.gather(*tasks)


async def initialize():
    devices = await ws.dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
    f = open(cfg.MAPPING, 'r')
    mapping = json.loads(f.read())
    f.close()
    tasks = []
    tasks.append(initialize_info())
    tasks.append(initialize_server())
    for d in devices:
        tasks.append(initialize_device(d, mapping['devices']))
        tasks.append(initialize_statuses(d, mapping['devices']))
    await asyncio.gather(*tasks)


@router.get('/api/integration/v1/reload')
async def reload_configuration():
    await initialize()
    return Response(status_code=204)
