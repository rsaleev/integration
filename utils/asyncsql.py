import aiomysql
import asyncio
import time


"""[summary]

Returns:
   AsyncDBPool object
     attributes:
       conn  - dictionary object with connection params
       
"""


class AsyncDBPool():

    def __init__(self, conn: dict):
        self.conn = conn
        self.pool = None
        self.connected = False

    async def connect(self):
        while self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(**self.conn, loop=asyncio.get_running_loop(), autocommit=True)
                self.connected = True
                return self
            except aiomysql.OperationalError as e:
                code, description = e.args
                if code == 2003 or 1053:
                    await asyncio.sleep(0.2)
                    continue
            except asyncio.TimeoutError:
                await asyncio.sleep(0.2)
                continue

    async def disconnect(self):
        self.pool.terminate()
        await self.pool.wait_closed()

    async def callproc(self, procedure: str,  rows: int, values: list = None):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    result = None
                    try:
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
                        await cur.close()
                        return result
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.2)
                        await self.connect()
                    except aiomysql.OperationalError as e:
                        code, description = e.args
                        if code == 2003 or 1053:
                            await asyncio.sleep(0.2)
                            await self.connect()
                        else:
                            raise e
        except:
            await asyncio.sleep(0.2)
            await self.connect()
