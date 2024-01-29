from typing import Tuple, List, Union
from functools import wraps
import psycopg2


class Database:
    """Class for working with database"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        """Creating connection to database"""
        self.conn = psycopg2.connect(host=host,
                                     port=port,
                                     database=database,
                                     user=user,
                                     password=password)
        self.cur = None

    def __del__(self) -> None:
        """Closing connection to database"""
        self.conn.close()

    def cursor(func):
        """Decorator for handling cursor object"""

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.cur is None:
                self.cur = self.conn.cursor()
            result = func(self, *args, **kwargs)
            return result

        return wrapper

    def save(self):
        """Method for saving result to database"""
        self.conn.commit()

    def cancel(self):
        """Method for cancelling result to database"""
        self.conn.rollback()

    @staticmethod
    def get_script(filepath: str) -> str:
        """Method for reading SQL script from file"""
        with open(filepath, encoding='utf-8-sig') as file:
            return file.read()

    @cursor
    def select(self, query: str) -> Tuple[List, List[Tuple]]:
        """Method for selecting data from database"""
        self.cur.execute(query)
        data = self.cur.fetchall()
        description = [x[0] for x in self.cur.description]
        return description, data

    @cursor
    def execute(self, query: str) -> int:
        """Method for execute SQL query"""
        self.cur.execute(query)
        rows = self.cur.rowcount
        return rows

    @cursor
    def insert(self, query: str, data: Union[list, tuple]) -> int:
        """Method for inserting data to database"""
        self.cur.executemany(query, data)
        rows = self.cur.rowcount
        return rows


if __name__ == '__main__':
    # pass
    trg_host = 'de-edu-db.chronosavant.ru'
    trg_port = 5432
    trg_database = 'edu'
    trg_user = 'deaian'
    trg_password = 'sarumanthewhite'
    db = Database(host=trg_host,
                  port=trg_port,
                  database=trg_database,
                  user=trg_user,
                  password=trg_password)

    print(db.select('select 1;'))
