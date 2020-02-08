import asyncio
import json
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import configuration as cfg
from datetime import datetime
from aiomysql import IntegrityError


async def app_init(eventloop):
    logger = await AsyncLogger().getlogger(cfg.log)
    logger.info("Establishing RDBS Wisepark Pool Connection...")
    dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=asyncio.get_running_loop()).connect()
    logger.info(f"RDBS Integration Connection: {dbconnector_wp.connected}")
    logger.info("Establishing RDBS Integration Pool Connection...")
    dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=asyncio.get_running_loop()).connect()
    logger.info(f"RDBS Integration Connection: {dbconnector_is.connected}")
    await dbconnector_is.callproc('is_clear', rows=0, values=[])
    logger.info(f"Reading device mapping file")

    try:
        with open(cfg.device_mapping) as f:
            mapping = json.load(f)
        devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
        ampp_id_mask = cfg.ampp_parking_id * 100
        await dbconnector_is.callproc('is_clear', rows=0, values=[])
        for dm in mapping['devices']:
            if dm['description'] == 'server':
                await dbconnector_is.callproc('is_devices_ins', rows=0, values=[0, 0, 0, dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], 0,
                                                                                cfg.server_ip, None, None, None, None,
                                                                                dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])
            for d in devices:
                if d['terAddress'] == dm['ter_addr']:
                    await dbconnector_is.callproc('is_devices_ins', rows=0, values=[d['terId'], d['terAddress'], d['terType'], dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], d['terIdArea'],
                                                                                    d['terIPV4'], d['terCamPlate1'], d['terCamPlate2'], d['terCamPhoto1'], d['terCamPhoto2'],
                                                                                    dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])
        devices_is = await dbconnector_is.callproc('is_devices_get', rows=-1, values=[])
        for di in devices_is:
            print(di)
            if di['terType'] == 0:
                for ds in mapping['statuses']['server']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terAddress'],
                                                                                   di['terType'], di['terDescription'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
            elif di['terType'] == 1:
                for ds in mapping['statuses']['entry']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terAddress'],
                                                                                   di['terType'], di['terDescription'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
            elif di['terType'] == 2:
                for ds in mapping['statuses']['exit']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terAddress'],
                                                                                   di['terType'], di['terDescription'], di['terIp'], di['amppId'], di['amppType'], ds, ''])
            elif di['terType'] == 3:
                for ds in mapping['statuses']['autocash']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[di['terId'], di['terAddress'],
                                                                                   di['terType'], di['terDescription'], di['terIp'], di['amppId'], di['amppType'], ds, ''])

        places = await dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
        with open(cfg.places_mapping) as f:
            mapped_places = json.load(f)
        for cp, p in zip(mapped_places['challenged'], places):
            if p['areId'] == cp['area']:
                await dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'], p['areDescription'], p['areTotalPark'], p['areFreePark'], cp['total']])
    except Exception as e:
        await logger.error(e)
        print(e)

loop = asyncio.get_event_loop()
loop.run_until_complete(app_init(loop))
