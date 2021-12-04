from pathlib import Path
from typing import Union

from katar.logger import logger
from katar.settings import *


class IndexFile:
    FILE_NAME = "index.katar"

    def __init__(self, path: Union[Path, None] = None) -> None:
        self.path = path

    def create_file(self, topicname):
        try:
            file_path = KATAR_DIR / topicname
            file_path.mkdir(parents=True, exist_ok=True)
            self.path: Path = file_path / self.FILE_NAME
            with open(self.path, "w") as fp:
                pass
            self.size = self.path.stat().st_size
        except Exception as e:
            logger.exception("Failed to create new file for index")
            return False
        return True

    def write_index(self, offset, location):
        index_insertion_string = f"{offset} {location}\n"
        try:
            with open(self.path, "a+") as index_file_appender:
                index_file_appender.write(index_insertion_string)
        except Exception as e:
            logger.exception("Failed to insert index")
            return False
        return True
