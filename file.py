from typing import Tuple, List
from datetime import datetime, date
import os
import pandas as pd


class File:
    """Class for handling files"""

    def __init__(self, filepath: str) -> None:
        self.path, self.filename, self.name, self.dt, self.ext = self.__split_name(filepath)
        self.headers, self.data = self.__HANDLER[self.ext](filepath)

    @staticmethod
    def __parse_dt(dt: str) -> date:
        """Method for converting date format"""
        return datetime.strptime(dt, '%d%m%Y').date()

    @staticmethod
    def __split_name(filepath: str) -> Tuple[str, str, str, date, str]:
        """Method for getting attributes from file name"""
        path = filepath.rsplit(os.sep, 1)[0]
        filename = filepath.rsplit(os.sep, 1)[-1]
        name, dt = filename.rsplit('.', 1)[0].rsplit('_', 1)
        ext = filename.rsplit('.', 1)[-1]
        return path, filename, name, File.__parse_dt(dt), ext

    @staticmethod
    def __read_txt(filepath: str) -> Tuple[Tuple[str, ...], List[List[str]]]:
        """Method for reading .txt files"""
        with open(filepath, encoding='utf-8-sig') as file:
            data = [row.strip().replace(',', '.').split(';') for row in file.readlines()]
            headers = tuple(data.pop(0))
        return headers, data

    @staticmethod
    def __read_xlsx(filepath: str) -> Tuple[Tuple[str, ...], List[List[str]]]:
        """Method for reading .xlsx files"""
        df = pd.read_excel(filepath)
        headers = tuple(df.columns.tolist())
        data = df.values.tolist()
        return headers, data

    __HANDLER = {'txt': __read_txt.__func__,
                 'xlsx': __read_xlsx.__func__}

    def __archive(self) -> None:
        """Method for archiving files"""
        old_path = f'{self.path}{os.sep}{self.filename}'
        new_path = f'{self.path}{os.sep}archive{os.sep}{self.filename}.backup'
        os.rename(old_path, new_path)

    def __del__(self):
        """Archiving file after closing"""
        self.__archive()


if __name__ == '__main__':
    pass
