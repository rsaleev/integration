
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
        self.__wsdl = None
        self.__method = str

    @property
    def method(self):
        return self.__method

    @method.setter
    def method(self, v):
        self.__method = v

    @method.getter
    def method(self):
        return self.__method

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
        return

    async def connect(self):
        while self.__transport is None or self.__client is None:
            try:
                self.__transport = AsyncTransport(loop=asyncio.get_running_loop(), cache=None, timeout=self.__timeout, operation_timeout=self.__timeout)
                async with self.__transport.session.get(self.__url) as resp:
                    content = await resp.read()
                    self.__wsdl = io.BytesIO(content)
                    self.__client = Client(wsdl=self.__wsdl, transport=self.__transport)
                    self.connected = True
                    return self
            except:
                continue

    async def disconnect(self):
        await self.__transport.session.close()

    async def execute(self, operation: str, header: bool = None, device: int = None, ** kwargs):
        if self.connected:
            operation = self.__client.service._operations[f'{operation}']
            msg_header = dict
            if header:
                msg_header = {'hIdMessage': int(datetime.now().timestamp()),
                              'hUser': self.__login,
                              'hPassw': self.__password,
                              'hDateTime': datetime.now().strftime("%Y%m%d%H%M%S"),
                              'hIdSite': cfg.object_id,
                              'hDevice': 0,
                              'hUserId': 0,
                              'hLanguage': 'ru'}
            if device:
                msg_header['hDevice'] = device
                res = await operation(sHeader=msg_header, **kwargs)
                return res
            else:
                res = await operation(**kwargs)
                return res
        else:
            self.connect()
