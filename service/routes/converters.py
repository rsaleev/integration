import aiohttp
import urllib3
import ssl
import configuration.settings as cs
from urllib.parse import urlencode
from fastapi.routing import APIRouter
from starlette.responses import JSONResponse
from aiohttp import ClientConnectionError, ClientTimeout, ClientError, ClientConnectorCertificateError, ClientResponseError
from pydantic import BaseModel
from typing import Optional
import asyncio

router = APIRouter()
name = 'converters'


async def troika_convert(code):
    # 4 bytes Mifare
    if len(code) == 8:
        b1 = code[0:2]
        b2 = code[2:4]
        b3 = code[4:6]
        b4 = code[6:8]
        pre_str = b4+b3+b2+b1
        came_code = str(int(pre_str, 16))
        if len(came_code) < 10:
            came_code = '00000000080' + came_code
        else:
            came_code = '0000000008' + came_code
        return came_code
    # 7 bytes
    elif len(code) == 14:
        byte_1 = str(int('8'+code[0:6], 16))
        if len(byte_1) == 9:
            byte_1_9 = byte_1[8:10]
            byte_1 = byte_1[0:8] + '0'+byte_1_9
        byte_2 = str(int(code[7:14], 16))
        if len(byte_2) == 8:
            byte_2_0 = byte_2[0:2]
            byte_2 = '0'+byte_2_0+byte_2[2:8]
        came_code = '0'+str(byte_1)+str(byte_2)
        return came_code


async def mifare_convert(code):
    b1 = code[0:2]
    b2 = code[2:4]
    b3 = code[4:6]
    b4 = code[6:8]
    pre_str = b1+b2+b3+b4
    came_code = str(int(pre_str, 16))
    if len(came_code) < 10:
        came_code = '00000000080' + came_code
    else:
        came_code = '0000000008' + came_code
    return came_code


class Card(BaseModel):
    card: Optional[dict]
    error: Optional[dict]


@router.get('/api/integration/v1/converters/troika/num/{num}')
async def troika_num(num):
    sslcontext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH,
                                            capath=cs.METRO_CERT_PATH)
    sslcontext.load_cert_chain(
        certfile=f'{cs.METRO_CERT_PATH}/ampp.crt',
        keyfile=f'{cs.METRO_CERT_PATH}/ampp.key')
    conn = aiohttp.TCPConnector(ssl_context=sslcontext)
    async with aiohttp.ClientSession(connector=conn) as session:
        try:
            async with session.get(url=f'{cs.METRO_INFO_URL}/get_card_by_num?num={num}', headers={"accept": "application/json"},
                                   timeout=cs.METRO_REQUEST_TIMEOUT, ssl=sslcontext) as r:
                if r.status == 200:
                    response = await r.json()
                    data = Card(**response)
                    data_out = {}
                    data_out['cardUid'] = data.card['uid']
                    data_out['cardNum'] = data.card['num']
                    data_out['cameCode'] = await troika_convert(data.card['uid'])
                    data_out['cardStatus'] = data.card['status']
                    return JSONResponse(data_out)
                elif r.status == 400:
                    response = await r.json()
                    data = Card(**response)
                    return JSONResponse(data.dict(exclude_unset=True))
        except (aiohttp.ClientConnectorError, aiohttp.ClientConnectorError, aiohttp.ClientConnectorCertificateError, TimeoutError) as e:
            return JSONResponse({'error': repr(e)})
        except asyncio.TimeoutError as e:
            return JSONResponse({'error': repr(e)})


@router.get('/api/integration/v1/converters/troika/uid/{uid}')
async def troika_uid(uid):
    sslcontext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH,
                                            capath=cs.METRO_CERT_PATH)
    sslcontext.load_cert_chain(
        certfile=f'{cs.METRO_CERT_PATH}/ampp.crt',
        keyfile=f'{cs.METRO_CERT_PATH}/ampp.key')
    conn = aiohttp.TCPConnector(ssl_context=sslcontext)
    async with aiohttp.ClientSession(connector=conn) as session:
        try:
            async with session.get(url=f'{cs.METRO_INFO_URL}/get_card_by_uid?uid={uid}', headers={"accept": "application/json"},
                                   timeout=cs.METRO_REQUEST_TIMEOUT, ssl=sslcontext) as r:
                if r.status == 200:
                    response = await r.json()
                    data = Card(**response)
                    data_out = {}
                    data_out['cardUid'] = data.card['uid']
                    data_out['cardNum'] = data.card['num']
                    data_out['cameCode'] = await troika_convert(data.card['uid'])
                    data_out['cardStatus'] = data.card['status']
                    return JSONResponse(data_out)
                elif r.status == 400:
                    response = await r.json()
                    data = Card(**response)
                    return JSONResponse(data_out)
        except (aiohttp.ClientConnectorError, aiohttp.ClientConnectorError, aiohttp.ClientConnectorCertificateError, TimeoutError) as e:
            return JSONResponse({'error': repr(e)})
        except asyncio.TimeoutError as e:
            return JSONResponse({'error': repr(e)})


@router.get('/api/integration/v1/converters/mifare/uid/{uid}')
async def mifare_uid(uid):
    try:
        data_out = {}
        data_out['card_uid'] = uid
        data_out['came_code'] = await mifare_convert(uid)
        return JSONResponse(data_out)
    except:
        return JSONResponse({'error': 'BAD_REQUEST'}, status_code=400)
