from pathlib import Path
from typing import Union

from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger
from katar.settings import *


class LogsFile:
    FILE_NAME = "logs.katar"

    def __init__(
        self, topicname: str, dirpath: Union[Path, None] = None, max_log_bytes: int = 4
    ) -> None:
        self.topicname = topicname
        self.max_log_bytes = max_log_bytes
        self.dirpath = dirpath if dirpath is not None else KATAR_DIR / self.topicname
        self.writer_init_file()

    def writer_init_file(self):
        try:
            self.dirpath.mkdir(parents=True, exist_ok=True)
            self.filepath: Path = self.dirpath / self.FILE_NAME
            if not self.filepath.is_file():
                with open(self.filepath, "w") as fp:
                    pass
            self.filesize = self.filepath.stat().st_size
        except Exception as e:
            logger.exception("Failed to create new file for index")
            return False
        return True

    def insert_log(self, log: bytes):
        logger.info(event="Log Insertion Request", message=log)
        log_length = len(log)
        log_length_in_bytes = int_to_bytes(
            value=log_length, byte_size=self.max_log_bytes
        )
        with open(self.filepath, "ab") as log_appender:
            offset_location = log_appender.tell()
            log_appender.write(log_length_in_bytes)
            log_appender.write(log)
        return offset_location

    def read_log(self, location):
        with open(self.filepath, "rb") as log_reader:
            log_reader.seek(location, 0)
            log_length_in_bytes = log_reader.read(4)
            log_length = int_from_bytes(log_length_in_bytes)
            log = log_reader.read(log_length).decode()
        return log
