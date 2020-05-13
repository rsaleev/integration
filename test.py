import asyncio
import re
import configuration as cfg
from utils.asyncsoap import AsyncSOAP
from utils.asyncsql import AsyncDBPool
import base64
from datetime import datetime


# async def test():
#     connector = await AsyncSOAP(cfg.soap_user, cfg.soap_password, cfg.object_id, cfg.soap_timeout, cfg.soap_url).connect()
#     res = await connector.execute(operation='GetPlate', header=True, device=1, wTerId=1)
#     print(res)

event = False



