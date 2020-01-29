
import io
import asyncio
from zeep.client import Client, Document
from zeep.asyncio.transport import AsyncTransport
from datetime import datetime


class AsyncSOAPClient:
    def __init__(self, login, password, parking_id):
        self.__transport = None
        self.__client = None
        self.__login = login
        self.__password = password
        self.__parking_id = parking_id
        self.__device = int
        self.connected = False

    async def connect(self):
        while self.__transport is None or self.__client is None:
            try:
                self.__transport = AsyncTransport(loop=asyncio.get_running_loop(), cache=None, timeout=soap_timeout, operation_timeout=soap_timeout)
                async with self.__transport.session.get(soap_url) as resp:
                    content = await resp.read()
                    wsdl = io.BytesIO(content)
                    self.__client = Client(wsdl, transport=self.__transport)
                    self.connected = True
            except:
                continue
        else:
            return self

    # Exclusive header for CAMAE SOAP service
    # Device is always 0 to mimic server request

    @property
    def deviceid(self):
        return self.__device

    @deviceid.getter
    def deviceid(self, v):
        self.__device = v

    @property
    def header(self):
        return {'hIdMessage': int(datetime.now().timestamp()),
                'hUser': self.__user,
                'hPassw': self.__password,
                'hDateTime': datetime.now().strftime("%Y%m%d%H%M%S"),
                'hIdSite': self.__objectid,
                'hDevice': 0,
                'hUserId': 0,
                'hLanguage': 'ru'}
