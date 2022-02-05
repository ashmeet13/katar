import traceback
from pathlib import Path

from katar.logger import logger


class BaseInteractor:
    def __init__(self) -> None:
        self.interactor_type: str = ""
        self.tracking_file: Path = None
        self.filesize: int = 0

    def set_tracking_file(self, topic_dir, base_offset, create_file=False):
        self.tracking_file = topic_dir / f"{base_offset}.{self.interactor_type}"
        if create_file:
            self.create()
        self.filesize = self.tracking_file.stat().st_size
        return self.filesize

    def create(self):
        try:
            if not self.tracking_file.is_file():
                with open(self.tracking_file, "w") as fp:
                    pass
        except Exception as e:
            logger.exception(event="Failed to create new file for index")
            return False
        return True

    def write(self, log, log_size):
        offset_location = None
        try:
            with open(self.tracking_file, "ab") as fp:
                offset_location = fp.tell()
                fp.write(log)
            self.filesize += log_size
        except Exception as e:
            raise e
        return offset_location, self.filesize
