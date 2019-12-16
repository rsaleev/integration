import aiomysql
import asyncio
import time


class AsyncDBPool():

    def __init__(self, size: int, name: str, conn: dict, loop: None):

        self.size = size
        self.name = name
        self.conn = conn
        self.pool = None
        self.connected = False
        self.loop = None
        self.cursor = aiomysql.DictCursor

    async def connect(self):
        while self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(minsize=1, maxsize=self.size, **self.conn, loop=self.loop, autocommit=True)
                if not self.pool is None:
                    self.connected = True
                return self
            except aiomysql.OperationalError as e:
                code, description = e.args
                if code == 2003:
                    continue
                else:
                    raise
            finally:
                return self

    def disconnect(self):
        try:
            self.pool.close()
            # self.pool.wait_closed()
        except:
            raise

    async def callproc(self, procedure: str, rows: None, values: list):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.callproc(procedure, [*values])
                    if rows and rows > 1:
                        result = await cur.fetchmany(rows)
                    elif rows and rows == 1:
                        result = await cur.fetchone()
                    else:
                        result = await cur.fetchall()
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                self._create()
            else:
                raise

    async def execute(self, size, stmt: str, *args):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(stmt, *args)
                    if size > 1:
                        result = await cur.fetchmany(size)
                        # cur.close()
                        # self.pool.release(conn)
                    elif size == 1:
                        result = await cur.fetchone()
                    else:
                        result = await cur.fetchall()
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                self._create()
            else:
                raise
