from functools import partial
from pathlib import Path

from katar.engine.io.base_interactor import BaseInteractor
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


class KatarInteractor(BaseInteractor):
    def __init__(self) -> None:
        super().__init__()
        self.interactor_type = "katar"

    def get_current_offset(self):
        offset = 0
        with open(self.tracking_file, "rb") as log_reader:
            for chunk in iter(partial(log_reader.read, 4), b""):
                log_length = int_from_bytes(chunk)
                log_reader.read(log_length)
                offset += 1

        return offset

    def get_size_from_location(self, location):
        size = 0
        ignore_first = True
        with open(self.tracking_file, "rb") as log_reader:
            log_reader.seek(location)
            for chunk in iter(partial(log_reader.read, 4), b""):
                log_length = int_from_bytes(chunk)
                log_reader.read(log_length)
                if ignore_first:
                    ignore_first = False
                    continue
                else:
                    size += log_length
        return size

    def print(self):
        print("Printing contents of file -", self.tracking_file)
        with open(self.tracking_file, "rb") as reader:
            for chunk in iter(partial(reader.read, 4), b""):

                length = int_from_bytes(chunk)
                payload_bytes = reader.read(length)

                print(length, payload_bytes)
