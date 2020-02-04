
import io
import asyncio
from zeep.client import Client, Document
from zeep.asyncio.transport import AsyncTransport
from datetime import datetime
import configuration as cfg


class AsyncSOAP:
    def __init__(self, login, password, parking_id, timeout, url):
        self.__transport = None
        self.__url = url
        self.__client = None
        self.__login = login
        self.__password = password
        self.__parking_id = parking_id
        self.__device = int
        self.__timeout = timeout
        self.connected = False

    async def connect(self):
        while self.__transport is None or self.__client is None:
            try:
                self.__transport = AsyncTransport(loop=asyncio.get_running_loop(), cache=None, timeout=self.__timeout, operation_timeout=self.__timeout)
                async with self.__transport.session.get(self.__url) as resp:
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

    @deviceid.setter
    def deviceid(self, v):
        self.__device = v

    @property
    def client(self):
        return self.__client

    @property
    def header(self):
        return {'hIdMessage': int(datetime.now().timestamp()),
                'hUser': self.__login,
                'hPassw': self.__password,
                'hDateTime': datetime.now().strftime("%Y%m%d%H%M%S"),
                'hIdSite': cfg.object_id,
                'hDevice': self.__device,
                'hUserId': 0,
                'hLanguage': 'ru'}
