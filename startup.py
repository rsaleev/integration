import asyncio
import json
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import configuration as cfg
from datetime import datetime
from aiomysql import IntegrityError


async def app_init(eventloop):
    print("Establishing RDBS Wisepark Pool Connection...")
    dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx, loop=asyncio.get_running_loop()).connect()
    print(f"RDBS Integration Connection: {dbconnector_wp.connected}")
    print("Establishing RDBS Integration Pool Connection...")
    dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx, loop=asyncio.get_running_loop()).connect()
    print(f"RDBS Integration Connection: {dbconnector_is.connected}")
    await dbconnector_is.callproc('is_clear', rows=0, values=[])
    print(f"Reading device mapping file")
    try:
        with open(cfg.device_mapping) as f:
            mapping = json.load(f)
        devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
        ampp_id_mask = cfg.ampp_parking_id * 100
        await dbconnector_is.callproc('is_clear', rows=0, values=[])
        for dm in mapping['devices']:
            if dm['description'] == 'server':
                await dbconnector_is.callproc('is_devices_ins', rows=0, values=[0, 0, dm['ter_type'], dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], 0,
                                                                                cfg.server_ip, None, None, None, None,
                                                                                dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])
            for d in devices:
                if d['terAddress'] == dm['ter_addr']:
                    await dbconnector_is.callproc('is_devices_ins', rows=0, values=[d['terId'], d['terAddress'], dm['ter_type'], dm['description'], ampp_id_mask+dm['ampp_id'], dm['ampp_type'], d['terIdArea'],
                                                                                    d['terIPV4'], d['terCamPlate1'], d['terCamPlate2'], d['terCamPhoto1'], d['terCamPhoto2'],
                                                                                    dm.get('imager', None), dm.get('payonline', None), dm.get('uniteller', None)])
        devices_is = await dbconnector_is.callproc('is_devices_get', rows=-1, values=[None, None, None, None, None])
        for di in devices_is:
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
        money = await dbconnector_wp.callproc('wp_money_get', rows=-1, values=[None])
        for m in money:
            device_is = next(d for d in devices_is if d['terId'] == m['curTerId'])
            await dbconnector_is.callproc('is_money_ins', rows=0, values=[device_is['terId'], device_is['terAddress'], device_is['terDescription'], device_is['amppId'], m['curChannelId'], m['curChannelDescr'], m['curQuantity'], m['curValue']])
        inventories = await dbconnector_wp.callproc('wp_inventory_get', rows=-1, values=[])
        for inv in inventories:
            device_is = next(d for d in devices_is if d['terId'] == inv['curTerId'])
            await dbconnector_is.callproc('is_inventory_ins', rows=0, values=[device_is['terId'], device_is['terAddress'],
                                                                              device_is['terDescription'], device_is['amppId'], inv['curChannelId'], inv['curChannelDescr'], inv['curTotal'], cfg.cashbox_limit])
    except Exception as e:
        print(e)

loop = asyncio.get_event_loop()
loop.run_until_complete(app_init(loop))
