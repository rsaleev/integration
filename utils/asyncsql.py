import aiomysql
import asyncio
import time


class AsyncDBPool():

    def __init__(self, conn: dict, loop: None):
        self.conn = conn
        self.pool = None
        self.connected = False
        self.loop = loop
        self.cursor = aiomysql.DictCursor

    async def connect(self):
        while self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(**self.conn, loop=self.loop, autocommit=True)
                if not self.pool is None:
                    self.connected = True
            except aiomysql.OperationalError as e:
                await asyncio.sleep(1)
                continue
            finally:
                return self

    async def callproc(self, procedure: str,  rows: int = None, values: list = None):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.callproc(procedure, [*values])
                    result = None
                    if rows == 1:
                        result = await cur.fetchone()
                    if rows > 1:
                        result = await cur.fetchmany(rows)
                    elif rows == -1:
                        result = await cur.fetchall()
                    elif rows == 0:
                        pass
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                self.connect()
            else:
                raise

    async def execute(self, stmt: str, rows: None, *args):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(stmt, *args)
                    result = None
                    if rows == 1:
                        result = await cur.fetchone()
                    elif rows == 0:
                        result = await cur.fetchall()
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                self.connect()
            else:
                raise
