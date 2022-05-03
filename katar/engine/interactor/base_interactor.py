import traceback
from pathlib import Path

from katar.constants import *
from katar.engine.metadata import Metadata
from katar.logger import logger


class BaseInteractor:
    def __init__(self, metadata: Metadata) -> None:
        """An interactor maintains all operations for a given file.
        An interactor tracks a specific file type in a katar database -
        1. Logs
        2. Index
        3. TimeIndex [Not yet implemented]
        """
        self.topic_dir: Path = metadata[MetadataKeys.TopicPath]
        self.interactor_type: str = ""
        self.tracking_file: Path = None
        self.filesize: int = 0

    def set_tracking_file(self, base_offset: int, create_file=False):
        self.tracking_file = self.topic_dir / f"{base_offset}.{self.interactor_type}"
        if create_file:
            self.create()
        self.filesize = self.tracking_file.stat().st_size

    def fetch_total_file_count(self):
        return len(list(self.topic_dir.glob("*")))

    def fetch_file_size(self):
        return self.filesize

    def file_exists(self, base_offset):
        filepath = self.topic_dir / f"{base_offset}.{self.interactor_type}"
        return filepath.is_file()

    def create(self):
        if not self.tracking_file.is_file():
            with open(self.tracking_file, "w") as fp:
                pass

    def write(self, log, log_size):
        offset_location = None
        with open(self.tracking_file, "ab") as fp:
            offset_location = fp.tell()
            fp.write(log)
        self.filesize += log_size
        return offset_location
