from functools import partial
from pathlib import Path

from katar.engine.io.base_interactor import BaseInteractor
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


def round_to_eight(x, base=8):
    return base * round(x / base)


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

    def print(self):
        print("Printing contents of file -", self.tracking_file)
        with open(self.tracking_file, "rb") as reader:
            for chunk in iter(partial(reader.read, 4), b""):

                location_in_bytes = reader.read(4)
                location = int_from_bytes(location_in_bytes)
                offset = int_from_bytes(chunk)

                print(offset, location)

    def find_location(self, offset, base_offset):
        target = offset - base_offset
        print()
        print("Trying to find -", target)
        print()
        with open(self.tracking_file, "rb") as reader:
            low_offset_bytes = reader.read(4)
            low_offset = int_from_bytes(low_offset_bytes)

            reader.seek(-8, 2)
            high_offset_bytes = reader.read(4)
            high_offset = int_from_bytes(high_offset_bytes)

            if target < low_offset:
                return 0
            elif target > high_offset:
                return int_from_bytes(reader.read(4))
            else:
                low_location = 0
                high_location = self.filesize - 8

                while low_location <= high_location:
                    mid_location = round_to_eight((low_location + high_location) // 2)
                    reader.seek(mid_location, 0)
                    mid_offset = int_from_bytes(reader.read(4))

                    print("Before", low_location, mid_location, high_location)
                    print(target, mid_offset)
                    if target == mid_offset:
                        return int_from_bytes(reader.read(4))
                    elif target > mid_offset:
                        low_location = mid_location + 8
                    elif target < mid_offset:
                        high_location = mid_location - 8

                    print("After", low_location, mid_location, high_location)

                reader.seek(high_location)
                return int_from_bytes(reader.read(4))
