import aiomysql
import asyncio
import time
from aiomysql import IntegrityError, ProgrammingError, OperationalError


class AsyncDBPool():

    def __init__(self, conn: dict, min_size: int = 5, max_size: int = 10):
        self.conn = conn
        self.pool = None
        self.connected = False

    async def connect(self):
        while self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(**self.conn, loop=asyncio.get_running_loop(), autocommit=True, connect_timeout=2)
                self.connected = True
                return self
            except aiomysql.OperationalError as e:
                code, description = e.args
                if code == 2003 or 1053:
                    await asyncio.sleep(0.2)
                    continue
                else:
                    raise e
            except (asyncio.TimeoutError, TimeoutError):
                await asyncio.sleep(0.2)
                continue

    async def disconnect(self):
        self.connected = False
        self.pool.terminate()
        await self.pool.wait_closed()

    async def callproc(self, procedure: str,  rows: int, values: list = None):
        if self.connected:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        result = None
                        await cur.callproc(procedure, [*values])
                        if rows == 1:
                            result = await cur.fetchone()
                        if rows > 1:
                            result = await cur.fetchmany(rows)
                        elif rows == -1:
                            data = await cur.fetchall()
                            if len(data) > 0:
                                result = data
                        elif rows == 0:
                            pass
                        return result
            except OperationalError as e:
                code, _ = e.args
                if code in [2003, 2013]:
                    self.connected = False
                    pass
                else:
                    raise e
