import asyncio
import json
from utils.asynclog import AsyncLogger
from utils.asyncsql import AsyncDBPool
import configuration as cfg
from datetime import datetime
from aiomysql import IntegrityError
from pprint import pprint


async def app_init(eventloop):
    print(cfg.CONFIGURATION)
    print("Establishing RDBS Wisepark Pool Connection...")
    print(cfg.wp_cnx)
    dbconnector_wp = await AsyncDBPool(conn=cfg.wp_cnx).connect()
    print(f"RDBS Integration Connection: {dbconnector_wp.connected}")
    print("Establishing RDBS Integration Pool Connection...")
    dbconnector_is = await AsyncDBPool(conn=cfg.is_cnx).connect()
    print(f"RDBS Integration Connection: {dbconnector_is.connected}")
    try:
        devices = await dbconnector_wp.callproc('wp_devices_get', rows=-1, values=[])
        # print(devices)
        ampp_id_mask = cfg.ampp_parking_id * 100
        f = open(cfg.MAPPING)
        mapping = json.loads(f.read())
        print("Allocating devices")
        for dm in mapping['devices']:
            if dm['ter_id'] == 0:
                await dbconnector_is.callproc('is_devices_ins', rows=0, values=[0, 0, 0, dm['description'], ampp_id_mask+dm['ampp_id'], 1, 1, cfg.server_ip])
                for s in dm['statuses']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[0, s])
            elif dm['ter_id'] > 0:
                dd = next(d for d in devices if d['terId'] == dm['ter_id'])
                await dbconnector_is.callproc('is_devices_ins', rows=0, values=[dd['terId'], dd['terAddress'], dd['terType'], dm['description'], ampp_id_mask+dm['ampp_id'], 1, 1, dd['terIPV4']])
                for s in dm['statuses']:
                    await dbconnector_is.callproc('is_status_ins', rows=0, values=[dd['terId'], s])
                if dd['terType'] in [1, 2]:
                    await dbconnector_is.callproc('is_columns_ins', rows=0, values=[dd['terId'], dd['terAddress'], dd['terType'], dd['terCamPlate'], dd['terCamPhoto1'], dd['terCamPhoto2']])
        print("Devices allocation done")
        print("Allocating places")
        places = await dbconnector_wp.callproc('wp_places_get', rows=-1, values=[None])
        # types:
        #  1 - commercial places
        #  2 - physically challenged places
        #  3 - subscription places
        for p in places:
            await dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'], p['areTotalPark'], p['areFreePark'], p['areType']])
        # zone for physically challenged places - type 2
        await dbconnector_is.callproc('is_places_ins', rows=0, values=[p['areId'], p['areFloor'], 0, 0, 2])
        print("Allocating inventories")
        money = await dbconnector_wp.callproc('wp_money_get', rows=-1, values=[None])
        if not money is None:
            for m in money:
                device_wp = next((d for d in devices if d['terId'] == m['curTerId']), None)
                device_is = next((dm for dm in mapping['devices'] if dm['ter_id'] == m['curTerId']), None)
                if not device_wp is None and not device_is is None:
                    await dbconnector_is.callproc('is_money_ins', rows=0, values=[m['curTerId'], device_wp['terAddress'], device_is['description'], device_is['ampp_id'], m['curChannelId'], m['curChannelDescr'], m['curQuantity'], m['curValue']])
        inventories = await dbconnector_wp.callproc('wp_inventory_get', rows=-1, values=[])
        if not inventories is None:
            for inv in inventories:
                device_wp = next((d for d in devices if d['terId'] == inv['curTerId']), None)
                device_is = next((dm for dm in mapping['devices'] if dm['ter_id'] == inv['curTerId']), None)
                if not device_wp is None and not device_is is None:
                    await dbconnector_is.callproc('is_inventory_ins', rows=0, values=[inv['curTerId'], device_wp['terAddress'],
                                                                                      device_is['description'], device_is['ampp_id'], inv['curChannelId'], inv['curChannelDescr'], device_is['capacity'], device_is['limit']])
        print("Inventories allocating done")
    except Exception as e:
        print(e)

loop = asyncio.get_event_loop()
loop.run_until_complete(app_init(loop))
