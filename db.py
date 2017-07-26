"""
DB HELPER
"""
import pymysql
import config
import logging

logger = logging.getLogger(f"{__name__}")


class DbHelper:
    """
    Class help to make clean primary code
    It runs all queries, fetches, connects, disconnects, commits etc. within itself :)
    Just use it like:

            >>> from db import DbHelper
            >>> db = DbHelper()

        SELECT example - 1 (clear query)
            >>> query = "SELECT * FROM table"
            >>> result = db.fetchone(query)

        SELECT example - 2 (query with data)
            >>> query = "SELECT * FROM table WHERE some_column = %s"
            >>> data = "your param"
            >>> result = db.fetchone(query, data)

        If you need to fetch some results from same query
        use this case on first query:
            >>> result = db.fetchone(query, data, open_cursor=True)
        and this case on the next one:
            >>> result = db.fetchone(query, data, next_result=True)
        don' forget to close_cursor after all
            >>> db.close_cursor()


    """
    def __init__(self):
        logger.debug("creating DbHelper")
        self.connection = self.connect()
        self.cursor = None

    @staticmethod
    def connect():
        try:
            connection = pymysql.connect(**config.db_settings)

        except pymysql.Error as e:
            logger.error(e)
            return None

        else:
            logger.info("Connection Established")
            return connection

    def get_cursor(self):
        try:
            self.cursor = self.connection.cursor()

        except pymysql.Error as e:
            logger.error(e)

        else:
            return self.cursor

    def close_cursor(self):
        if self.cursor:
            self.cursor.close()

    def fetchone(self, *args, **kwargs):
        if self.connection and args:
            if self.cursor:  # if cursor is opened

                if kwargs.get('next_result'): # if you don' need to run query again, just need to fetch next result
                    result = self.cursor.fetchone()

                else:  # if cursor is open but you need to run query again
                    self.cursor.execute(*args)
                    result = self.cursor.fetchone()

            elif kwargs.get('open_cursor'):  # if kwarg open_cursor is True - open cursor
                cursor = self.get_cursor()
                cursor.execute(*args)
                result = cursor.fetchone()

            else:  # if you need to open temp cursor and close it after fetch
                with self.connection.cursor() as cursor:
                    cursor.execute(*args)
                    result = cursor.fetchone()

            return result

    def fetchall(self, *args):
        if self.connection and args:
            with self.connection.cursor() as cursor:
                cursor.execute(*args)
                return cursor.fetchall()

    def update(self, *args, **kwargs):
        if self.connection:
            if args:
                with self.connection.cursor() as cursor:
                    cursor.execute(*args)

            if kwargs.get('no_commit'):
                pass
            else:
                self.connection.commit()

    def commit(self):
        if self.connection:
            self.connection.commit()

    def close(self):
        if self.connection:
            self.connection.close()

        if self.cursor:
            self.cursor.close()
