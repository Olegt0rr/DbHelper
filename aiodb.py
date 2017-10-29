"""
DB HELPER
"""
import aiomysql
from aiomysql.connection import Connection, Cursor
import asyncio
import time
import config
import logging

logger = logging.getLogger(f"{__name__}")
logger.setLevel(logging.INFO)


class DbHelper:

    def __init__(self, loop=None):
        logger.debug("db: creating DbHelper instance")
        self.connection: Connection = None
        self.cursor: Cursor = None
        self._loop = loop or asyncio.get_event_loop()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        tries = 0
        while tries < 3:
            tries += 1
            try:
                self.connection: Connection = await aiomysql.connect(loop=self._loop, **config.db_settings)

            except Exception as e:
                logger.error(f"db: can't connect: \n{e}")
                time.sleep(1)
                pass

            else:
                logger.debug('db: connection established')

    async def get_cursor(self):
        try:
            self.cursor = await self.connection.cursor()

        except Exception as e:
            logger.error(f'db error with cursor open: \n{e}')

        else:
            logger.debug('db: cursor opened')
            return self.cursor

    async def close_cursor(self):
        if self.cursor:
            await self.cursor.close()

    async def fetchone(self, *args, **kwargs):
        if isinstance(self.connection, Connection) and not self.connection.closed and args:
            if isinstance(self.cursor, Cursor) and not self.cursor.closed:  # if cursor is opened

                if kwargs.get('next_result'):  # if you don' need to run query again, just need to fetch next result
                    result = await self.cursor.fetchone()

                else:  # if cursor is open but you need to run query again
                    await self.cursor.execute(*args)
                    result = await self.cursor.fetchone()

            elif kwargs.get('open_cursor'):  # if kwarg open_cursor is True - open cursor
                cursor = await self.get_cursor()
                await cursor.execute(*args)
                result = await cursor.fetchone()

            else:  # if you need to open temp cursor and close it after fetch
                async with self.connection.cursor() as cursor:
                    await cursor.execute(*args)
                    result = await cursor.fetchone()

            return result

    async def fetchall(self, *args, **kwargs):
        if isinstance(self.connection, Connection) and not self.connection.closed and args:
            async with self.connection.cursor() as cursor:
                await cursor.execute(*args)
                return await cursor.fetchall()

    async def update(self, *args, no_commit=None, **kwargs):
        try:
            if isinstance(self.connection, Connection) and not self.connection.closed:
                if args:
                    async with self.connection.cursor() as cursor:
                        await cursor.execute(*args)

                    if no_commit:
                        pass
                    else:
                        await self.connection.commit()

        except Exception as e:
            logger.error(f'db: update failed (pysql reason) \n{e}')
            return False

        else:
            logger.debug(f'db: update successful')
            return True

    async def insert(self, *args, no_commit=None, **kwargs):
        try:
            if isinstance(self.connection, Connection) and not self.connection.closed:
                if args:
                    async with self.connection.cursor() as cursor:
                        await cursor.execute(*args)

                    if no_commit:
                        pass
                    else:
                        await self.commit()

        except Exception as e:
            logger.error(f'db: insert failed: \n{e}')
            return False

        else:
            logger.debug(f'db: insert successful')
            return True

    async def commit(self):
        if self.connection:
            await self.connection.commit()
            logger.debug(f'db: commit successful')

    async def close(self):
        if isinstance(self.cursor, Cursor) and not self.cursor.closed:
            await self.cursor.close()
            logger.debug(f'db: cursor closed')

        if isinstance(self.connection, Connection) and not self.connection.closed:
            self.connection.close()
            logger.debug(f'db: connection closed')
