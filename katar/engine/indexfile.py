from pathlib import Path
from typing import Union

from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger
from katar.settings import KATAR_DIR


class IndexFile:
    FILE_NAME = "index.katar"

    def __init__(
        self,
        topicname: str,
        dirpath: Union[Path, None] = None,
        max_offset_bytes: int = 4,
        max_log_bytes: int = 4,
    ) -> None:
        self.topicname = topicname
        self.dirpath = dirpath if dirpath is not None else KATAR_DIR / self.topicname
        self.max_offset_bytes = max_offset_bytes
        self.max_log_bytes = max_log_bytes

    def init_file(self):
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

    def init_offset(self):
        if self.__getattribute__("filesize") is None:
            logger.exception("File not initalised")
            return False

        if self.filesize == 0:
            self.offset = 0
            return True

        last_offset_location = self.filesize - (
            self.max_offset_bytes + self.max_log_bytes
        )
        print("Last location =", last_offset_location)
        with open(self.filepath, "rb") as index_reader:
            index_reader.seek(last_offset_location, 0)
            offset = index_reader.read(self.max_offset_bytes)
        self.offset = int_from_bytes(offset) + 1
        return True

    def write_index(self, location):
        offset_in_bytes = int_to_bytes(
            value=self.offset, byte_size=self.max_offset_bytes
        )
        location_in_bytes = int_to_bytes(value=location, byte_size=self.max_log_bytes)

        try:
            with open(self.filepath, "ab") as index_appender:
                offset_location = index_appender.tell()
                index_appender.write(offset_in_bytes)
                index_appender.write(location_in_bytes)
        except Exception as e:
            logger.exception("Failed to insert index")
            return False
        self.offset += 1
        return self.offset - 1
