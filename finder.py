from typing import Tuple, List, Iterator
import os
import glob


class FileFinder:
    """Class for searching files by template"""

    def __init__(self, path: str, templates: Tuple[str, ...]) -> None:
        self.__path = path
        self.__templates = templates
        self.__files = self.__find(self.__generate_filename(path, templates))

    @staticmethod
    def __generate_filename(path: str, templates: Tuple[str, ...]) -> List[str]:
        """Method for concatenating path and template"""
        return [f'{path}{os.sep}{template}' for template in templates]

    @staticmethod
    def __find(filenames: List[str]) -> List[str]:
        """Method for searching files by template"""
        return [filepath for filename in filenames for filepath in glob.glob(filename)]

    def __iter__(self) -> Iterator[str]:
        for name in self.__files:
            yield name


if __name__ == '__main__':
    pass
