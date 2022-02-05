from functools import partial
from pathlib import Path

from katar.engine.io.base_interactor import BaseInteractor
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


class IndexInteractor(BaseInteractor):
    def __init__(self) -> None:
        super().__init__()
        self.interactor_type = "index"

    def get_last_location(self):
        location = 0
        with open(self.tracking_file, "rb") as reader:
            for chunk in iter(partial(reader.read, 4), b""):
                location_in_bytes = reader.read(4)
                location = int_from_bytes(location_in_bytes)

        return location
