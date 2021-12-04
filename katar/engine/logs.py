from pathlib import Path
from typing import Union

from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger
from katar.settings import *


class LogsFile:
    FILE_NAME = "logs.katar"

    def __init__(self, path: Union[Path, None] = None) -> None:
        self.path = path

    def create_file(self, topicname):
        try:
            file_path = KATAR_DIR / topicname
            file_path.mkdir(parents=True, exist_ok=True)
            self.path: Path = file_path / self.FILE_NAME
            if not self.path.is_file():
                with open(self.path, "w") as fp:
                    pass
            self.size = self.path.stat().st_size
        except Exception as e:
            logger.exception("Failed to create new file for index")
            return False
        return True

    def insert_log(self, log_bytes: bytes):
        log_length = int_to_bytes(len(log_bytes))
        with open(self.path, "ab") as log_appender:
            offset_location = log_appender.tell()
            log_appender.write(log_length)
            log_appender.write(log_bytes)
        return offset_location
